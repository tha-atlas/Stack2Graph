"""Export a fixed snapshot through the source, mapping, and artifact APIs."""

import json
from collections.abc import Callable, Iterable, Iterator
from contextlib import ExitStack
from dataclasses import replace
from hashlib import sha256
from importlib.metadata import version
from pathlib import Path
from typing import Literal

from pyoxigraph import NamedNode, Quad, RdfFormat, parse

from graph.adapter.budgets import ArtifactBudget
from graph.adapter.diagnostics import write_failure
from graph.adapter.inventory import file_ref, merge_tag_usage
from graph.adapter.postgres_source import PostgreSQLGraphSource
from graph.adapter.shards import GraphShardWriter
from graph.domain.artifacts import (
    RESERVED_GRAPH_KEYS,
    ArtifactLayout,
    ArtifactManifest,
    RangeMarker,
    combine_processed_table_counts,
    processed_table_counts_from_json,
    require_processed_table_counts,
)
from graph.domain.config import ExportConfig, RunIdentity
from graph.domain.events import CommentEvent, LinkEvent, PostEvent, QuestionEvent, QuestionStart
from graph.domain.source import IMPORTED_TABLES, QuestionRange, question_ranges
from graph.mapping.content import ContentMapper
from graph.mapping.identity import NAMESPACE, IriPolicy
from graph.mapping.questions import QuestionMapper
from graph.mapping.shared import SharedMapper
from graph.resources import GraphResourcePlanner
from graph.schema.contract import SchemaContract
from graph.schema.ontology import new_schema_graph
from graph.services.parallel import ParallelExporter, RangeJob
from graph.services.recovery import RecoveryPlan, recover
from graph.services.shared_records import SharedRecordExporter
from graph.services.validate import read_manifest
from selection.languages import LanguageSelector
from selection.registry import load_selection_registry
from utils.artifacts import AtomicBinaryWriter, atomic_write_json
from utils.config import validate_item_id
from utils.logging_utils import ProgressReporter, get_logger, pipeline_run, stage_event
from utils.manifest import SnapshotLock
from utils.resources import ResourcePlan, detect_limits, plan_resources
from utils.types import JSONObject, json_object, parse_json


def exporter_digest() -> str:
    """Hash graph and selection source files with RDF library versions."""
    root = Path(__file__).resolve().parents[2]
    digest = sha256()
    for directory in (root / "graph", root / "selection"):
        for path in sorted(directory.rglob("*.py")):
            digest.update(path.relative_to(root).as_posix().encode())
            digest.update(path.read_bytes())
    for name in ("pyoxigraph", "rdflib"):
        digest.update((name + version(name)).encode())
    return digest.hexdigest()


def canonical_ontology_text() -> str:
    """Return sorted ontology N-Triples for both hashing and export."""
    return "".join(sorted(new_schema_graph(NAMESPACE).serialize(format="nt").splitlines(keepends=True)))


def _shard_writer(
    layout: ArtifactLayout, prefix: str, config: ExportConfig, resources: ResourcePlan, *, max_open_files: int
) -> GraphShardWriter:
    """Create a shard writer with run compression and a stage file budget."""
    return GraphShardWriter(
        layout,
        prefix,
        target_bytes=config.shard_bytes,
        max_open_files=max_open_files,
        compression=config.compression,
        compression_level=config.compression_level,
        resources=resources,
    )


def build_identity(config: ExportConfig, source: PostgreSQLGraphSource) -> RunIdentity:
    """Bind snapshot, registry, ontology, exporter, and artifact settings."""
    schema = Path(__file__).resolve().parents[1] / "schema/source.txt"
    ontology_digest = sha256(canonical_ontology_text().encode()).hexdigest()
    return RunIdentity(
        source.reader.snapshot_identity.item_id,
        source.reader.snapshot_identity.digest,
        source.selector.registry.digest,
        sha256(schema.read_bytes()).hexdigest(),
        ontology_digest,
        "kg-map-5-streamed-count-acceptance",
        exporter_digest(),
        config.range_width,
        config.compression,
        config.compression_level,
        config.shard_bytes,
    )


def with_question_progress(
    events: Iterable[QuestionEvent],
    progress: Callable[[int], None] | None,
    *,
    on_question: Callable[[QuestionStart], None] | None = None,
) -> Iterator[QuestionEvent]:
    """Forward graph events while reporting every question boundary."""
    completed = 0
    for event in events:
        if isinstance(event, QuestionStart):
            completed += 1
            if on_question is not None:
                on_question(event)
            if progress is not None:
                progress(completed)
        yield event


def export_range(
    source: PostgreSQLGraphSource,
    content: ContentMapper,
    layout: ArtifactLayout,
    part: QuestionRange,
    resources: ResourcePlan,
    shard_bytes: int,
    compression: Literal["none", "gzip"] = "none",
    compression_level: int | None = None,
    progress: Callable[[int], None] | None = None,
) -> RangeMarker:
    """
    Publish a range's graph shards, memberships, usage, and row counts.

    Args:
        source: Pinned question-event source.
        content: Ontology-backed content mapper.
        layout: Destination artifact layout.
        part: Half-open question ID range to export.
        resources: Memory, batch, spill, and disk limits for this worker.
        shard_bytes: Target uncompressed bytes before rotating a graph shard.
        compression: Stored shard encoding, either none or gzip.
        compression_level: Explicit gzip level when compression is enabled.
        progress: Optional callback receiving the completed-question count.

    Returns:
        Published range marker with artifact references and source-row counts.

    Raises:
        ValueError: Source events, graph identities, or resource limits are invalid.

    """
    budget = ArtifactBudget.from_resources(resources)
    usage = AtomicBinaryWriter(layout.path(f"usage/{part.key}.jsonl"))
    membership = AtomicBinaryWriter(layout.path(f"membership/{part.key}.jsonl"))
    shards = GraphShardWriter(
        layout,
        part.key,
        target_bytes=shard_bytes,
        max_open_files=8,
        compression=compression,
        compression_level=compression_level,
        resources=resources,
    )
    count = link_rows = distinct_links = 0
    processed: dict[str, int] = dict.fromkeys(IMPORTED_TABLES, 0)

    def record_question(event: QuestionStart) -> None:
        """Count a question and write its graph memberships and tag usages."""
        nonlocal count
        count += 1
        member: JSONObject = {"question_id": event.post.id, "graph_keys": list(event.graph_keys)}
        budget.write_bytes(
            membership.stream,
            layout.root,
            (json.dumps(member, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        )
        for tag in event.tags:
            for key in event.graph_keys:
                budget.write_bytes(
                    usage.stream,
                    layout.root,
                    (json.dumps((tag.id, key), separators=(",", ":")) + "\n").encode(),
                )

    def tracked() -> Iterator[QuestionEvent]:
        """Yield source events while counting each successfully processed row."""
        nonlocal count, link_rows, distinct_links
        for event in with_question_progress(
            source.iter_questions(part, resources.batches), progress, on_question=record_question
        ):
            # Each event carries one successfully processed source record (aggregated link
            # events carry their row multiplicity), counted here rather than copied
            # from inventory. Graph copies never duplicate a row: one event per source record.
            if isinstance(event, (QuestionStart, PostEvent)):
                processed["Posts"] += 1
                processed["Votes"] += event.votes.up + event.votes.down
            elif isinstance(event, CommentEvent):
                processed["Comments"] += 1
            elif isinstance(event, LinkEvent):
                processed["PostLinks"] += event.source_rows
                link_rows += event.source_rows
                distinct_links += 1
            yield event

    try:
        shards.write(QuestionMapper(content).map(tracked()))
        catalog = shards.finish()
        usage.publish()
        membership.publish()
        marker = RangeMarker(
            part,
            catalog,
            count,
            file_ref(layout, usage.path),
            file_ref(layout, membership.path),
            require_processed_table_counts(tuple(processed.items())),
            link_rows,
            distinct_links,
        )
        budget.write_json(layout.range_marker(part.key), marker.to_json())
        return marker
    except (ValueError, RuntimeError, OSError) as error:
        build_path = layout.path("build.json")
        identity = (
            RunIdentity.from_json(json_object(json_object(parse_json(build_path.read_text()))["identity"]))
            if build_path.exists()
            else None
        )
        write_failure(layout, identity, part.key, error)
        raise
    finally:
        shards.discard()
        usage.discard()
        membership.discard()


class GraphExporter:
    def __init__(self, config: ExportConfig) -> None:
        """Store the graph export configuration."""
        self.config = config

    def _source(self, resources: ResourcePlan) -> PostgreSQLGraphSource:
        """Open a single-connection graph source for the configured snapshot."""
        return PostgreSQLGraphSource(
            self.config.paths,
            resources,
            replace(self.config.postgres, pool_size=1),
            LanguageSelector(load_selection_registry(self.config.registry_path)),
            IriPolicy(),
            snapshot_id=self.config.snapshot_id,
        )

    def _replanned_source(
        self, resources: ResourcePlan, identity: RunIdentity, *, context: str
    ) -> PostgreSQLGraphSource:
        """Rebuild the source under replanned resources and re-confirm the run identity."""
        source = self._source(resources)
        try:
            identity.require_match(build_identity(self.config, source), context=context)
        except BaseException:
            source.close()
            raise
        return source

    def _export_shared(
        self,
        layout: ArtifactLayout,
        source: PostgreSQLGraphSource,
        recovery: RecoveryPlan | None,
        content: ContentMapper,
        usage_paths: Callable[[], Iterator[Path]],
        resources: ResourcePlan,
        budget: ArtifactBudget,
    ) -> tuple[tuple[str, int], ...]:
        """Run the shared tags/documentation stage or reuse its checkpointed counters."""
        if recovery is None or not recovery.shared_complete:
            get_logger(__name__).info("Exporting shared tags and documentation")
            shards = _shard_writer(layout, "shared", self.config, resources, max_open_files=8)
            try:
                shared = SharedRecordExporter(source, SharedMapper(content)).run(
                    merge_tag_usage(
                        usage_paths(),
                        resources.batches.max_bytes,
                        budget=ArtifactBudget.from_resources(resources),
                    ),
                    shards,
                    resources.batches,
                )
                budget.write_json(
                    layout.path("shared.json"),
                    {
                        "catalog": shared.catalog.to_json(),
                        "processed_table_counts": [[table, count] for table, count in shared.processed_table_counts],
                    },
                )
                return shared.processed_table_counts
            finally:
                shards.discard()
        # A checkpointed shared pass contributes its durable counters without rereading
        # tag or documentation artifacts.
        shared_document = json_object(parse_json(layout.path("shared.json").read_text()))
        return processed_table_counts_from_json(shared_document["processed_table_counts"])

    def _export_schema(
        self, layout: ArtifactLayout, resources: ResourcePlan, iris: IriPolicy, budget: ArtifactBudget
    ) -> None:
        """Publish ontology quads once in the schema named graph."""
        if layout.path("schema.json").exists():
            return
        get_logger(__name__).info("Exporting ontology")
        writer = _shard_writer(layout, "schema", self.config, resources, max_open_files=1)
        try:
            writer.write(
                Quad(q.subject, q.predicate, q.object, NamedNode(iris.graph("schema")))
                for q in parse(canonical_ontology_text(), RdfFormat.N_TRIPLES)
            )
            budget.write_json(layout.path("schema.json"), {"catalog": writer.finish().to_json()})
        finally:
            writer.discard()

    def run(self, run: str, *, resume: bool = False) -> ArtifactManifest:
        """Export or resume a named graph run with scoped logging."""
        with pipeline_run("export", log_dir=self.config.paths.data_root / "graph-reports", reuse_active=True):
            return self._export(run, resume=resume)

    def _export(self, run: str, *, resume: bool) -> ArtifactManifest:
        """Coordinate locked range, shared-content, and schema export stages."""
        validate_item_id(run)
        layout = ArtifactLayout(self.config.run_root / run)
        layout.root.mkdir(parents=True, exist_ok=True)
        detected = detect_limits()
        resources = plan_resources(detected, self.config.limits, scalable=True)
        iris = IriPolicy()
        source = self._source(resources)
        identity: RunIdentity | None = None
        try:
            with SnapshotLock(layout.root) as lock:
                if layout.path("build.json").exists() and not resume:
                    raise ValueError("Run already exists; use resume to preserve completed work")
                identity = build_identity(self.config, source)
                inventory = source.describe()
                graph_plan = GraphResourcePlanner(self.config.limits).plan(detected, source.database_capacity())
                exclusions = {code: count for code, count in inventory.exclusions if count}
                if exclusions:
                    get_logger(__name__).warning("Applying declared source exclusions: %s", exclusions)
                    stage_event("source_exclusions", exclusions=exclusions)
                get_logger(__name__).info(
                    "Exporting %s questions with %s workers", inventory.question_count, graph_plan.workers
                )
                stage_event("export_plan", questions=inventory.question_count, workers=graph_plan.workers)
                resources = graph_plan.per_worker
                budget = ArtifactBudget.from_resources(resources)
                source.close()
                source = self._replanned_source(resources, identity, context="While determining worker budgets")
                recovery: RecoveryPlan | None = None
                if layout.path("build.json").exists():
                    recovery_progress = ProgressReporter(
                        get_logger(__name__), "Resume validation", unit="questions", total=inventory.question_count
                    )
                    with recovery_progress.track():
                        recovery = recover(layout, identity, lock=lock, progress=recovery_progress.update)
                        recovery_progress.update(recovery.completed_questions, force=True)
                else:
                    if resume:
                        raise ValueError("Cannot resume an unknown build")
                    budget.write_json(
                        layout.path("build.json"),
                        {
                            "identity": identity.to_json(),
                            "first_question": inventory.first_question_id,
                            "last_question": inventory.last_question_id,
                            "questions": inventory.question_count,
                        },
                    )
                contract = SchemaContract.from_ontology(new_schema_graph(NAMESPACE))
                content = ContentMapper(contract, iris)
                if recovery is not None and layout.manifest.exists():
                    existing = read_manifest(layout)
                    if existing.complete:
                        return existing
                atomic_write_json(
                    layout.path("resources.json"),
                    {
                        "workers": graph_plan.workers,
                        "pending_jobs": graph_plan.pending_jobs,
                        "total_memory_bytes": graph_plan.total_memory_bytes,
                        "connection_budget": graph_plan.connection_budget,
                        "per_worker": graph_plan.per_worker.to_json(),
                        "coordinator": graph_plan.coordinator.to_json(),
                    },
                )
                completed = recovery.completed_questions if recovery is not None else 0
                export_progress = ProgressReporter(
                    get_logger(__name__),
                    "Export",
                    unit="questions",
                    initial=completed,
                    total=inventory.question_count,
                )
                catalog_progress = ProgressReporter(
                    get_logger(__name__), "Range catalog", unit="questions", total=inventory.question_count
                )
                processed_groups: list[tuple[tuple[str, int], ...]] = []
                with ExitStack() as progress_stack:
                    tracked_export = progress_stack.enter_context(export_progress.track())
                    progress_stack.enter_context(catalog_progress.track(shared_display=tracked_export.display))
                    if graph_plan.workers > 1:

                        def jobs() -> Iterator[RangeJob]:
                            """Yield export jobs for question ranges without completion markers."""
                            for part in question_ranges(inventory, self.config.range_width):
                                if not layout.range_marker(part.key).exists():
                                    yield RangeJob(
                                        replace(self.config, run_root=layout.root),
                                        identity,
                                        part,
                                        graph_plan.per_worker,
                                    )

                        for marker in ParallelExporter().run(
                            jobs(), graph_plan, progress=export_progress.update, initial=completed
                        ):
                            completed += marker.question_count
                    ranges = AtomicBinaryWriter(layout.path("ranges.jsonl"))
                    cataloged = 0
                    try:
                        for part in question_ranges(inventory, self.config.range_width):
                            marker_path = layout.range_marker(part.key)
                            if marker_path.exists():
                                marker = RangeMarker.from_json(json_object(parse_json(marker_path.read_text())))
                            else:

                                def update_range(range_completed: int) -> None:
                                    """Add current range progress to the count of completed questions."""
                                    export_progress.update(completed + range_completed)

                                marker = export_range(
                                    source,
                                    content,
                                    layout,
                                    part,
                                    resources,
                                    self.config.shard_bytes,
                                    self.config.compression,
                                    self.config.compression_level,
                                    update_range,
                                )
                                completed += marker.question_count
                                export_progress.update(completed)
                            # Ranges are cataloged exactly once here, whether freshly exported,
                            # produced by parallel workers, or recovered from disk. Their
                            # processed counters therefore enter the ledger exactly once.
                            processed_groups.append(marker.processed_table_counts)
                            budget.write_bytes(
                                ranges.stream,
                                layout.root,
                                (json.dumps(marker.to_json(), sort_keys=True, separators=(",", ":")) + "\n").encode(),
                            )
                            cataloged += marker.question_count
                            catalog_progress.update(cataloged)
                        ranges.publish()
                        export_progress.update(completed, force=True)
                        catalog_progress.update(cataloged, force=True)
                    finally:
                        ranges.discard()

                def usage_paths() -> Iterator[Path]:
                    """Yield tag-usage file paths from the committed range catalog."""
                    with ranges.path.open() as stream:
                        for line in stream:
                            marker = RangeMarker.from_json(json_object(parse_json(line)))
                            yield layout.path(marker.usage.path)

                source.close()
                resources = graph_plan.coordinator
                budget = ArtifactBudget.from_resources(resources)
                source = self._replanned_source(resources, identity, context="Before coordinator stages")
                processed_groups.append(
                    self._export_shared(layout, source, recovery, content, usage_paths, resources, budget)
                )
                self._export_schema(layout, resources, iris, budget)
                manifest = ArtifactManifest(
                    identity,
                    inventory,
                    tuple(sorted((*source.selector.registry.base_tags, *RESERVED_GRAPH_KEYS))),
                    file_ref(layout, ranges.path),
                    file_ref(layout, layout.path("shared.json")),
                    file_ref(layout, layout.path("schema.json")),
                    True,
                    combine_processed_table_counts(processed_groups),
                )
                budget.write_json(layout.manifest, manifest.to_json())
                return manifest
        except (ValueError, RuntimeError, OSError) as error:
            write_failure(layout, identity, None, error)
            raise
        finally:
            source.close()
