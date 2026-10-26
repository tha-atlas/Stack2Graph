"""Run public count checks before finalizing a vector build."""

import logging
from collections.abc import Mapping
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Protocol

from graph.adapter.postgres_source import PostgreSQLGraphSource
from graph.mapping.identity import IriPolicy
from selection.languages import LanguageSelector
from selection.registry import load_selection_registry
from sql.adapter.reader import PostgreSQLSnapshotReader
from utils.resources import BatchLimits, ResourcePlan, detect_limits
from utils.types import JSONObject
from validation import validate_graph_postgres, validate_graph_qdrant, validate_postgres_qdrant
from validation.adapter.graph import GraphArtifactAdapter, graph_completed
from validation.adapter.postgres import PostgreSQLCountAdapter
from validation.adapter.reports import load_reports
from validation.adapter.vector import vector_artifact_evidence
from validation.domain.models import (
    GraphArtifactEvidence,
    QdrantCollectionEvidence,
    SnapshotTarget,
    TableCounts,
    ValidationMismatchError,
)
from validation.domain.report import Observations, parse_observations, validated_for_target
from validation.domain.results import CheckResults, prerequisite_comparison
from validation.domain.stages import GRAPH_POSTGRES, GRAPH_QDRANT, POSTGRES_QDRANT
from validation.services.counterparts import select_completed_run
from validation.services.embedded import (
    EmbeddedCheck,
    EmbeddedSkip,
    embedded_validation_paths,
    run_embedded_validation,
)
from validation.services.reporting import ReportArtifact
from vector.domain.config import RunConfig
from vector.domain.models import CollectionState
from vector.loading.finalize import FinalizationValidator
from vector.shards import Manifest

LOGGER = logging.getLogger(__name__)


class PostgresWindowCounter(Protocol):
    """Count a fixed snapshot's question window, with offline substitution."""

    def __call__(self, *, snapshot_id: str, after_id: int, limit: int | None) -> int:
        """Count questions in the pinned snapshot's requested source window."""
        ...


class GraphExportCounts(Protocol):
    """Count source rows using graph export filters, with offline substitution."""

    def __call__(self) -> TableCounts:
        """Count source rows using the graph export's filters."""
        ...


def _reader_resources(config: RunConfig) -> ResourcePlan:
    """Build a single-thread reader plan from vector validation limits."""
    limits = config.limits
    return ResourcePlan(
        1,
        limits.read_bytes,
        limits.read_bytes // 4,
        BatchLimits(limits.read_rows, limits.read_bytes, limits.read_bytes),
        1,
        1,
        detect_limits().evidence,
    )


def _postgres_window_counter(config: RunConfig) -> PostgresWindowCounter:
    """Build a callback that opens a pinned reader for each count request."""

    def count(*, snapshot_id: str, after_id: int, limit: int | None) -> int:
        """Count a snapshot's question window and close the adapter afterward."""
        reader = PostgreSQLSnapshotReader(
            config.paths,
            _reader_resources(config),
            config.postgres,
            snapshot_id=snapshot_id,
            read_attempts=config.limits.attempts,
        )
        with closing(PostgreSQLCountAdapter(reader)) as adapter:
            return adapter.window_question_count(after_id=after_id, limit=limit)

    return count


def _graph_export_counts(config: RunConfig) -> GraphExportCounts:
    """Count the pinned source using the graph export's shared filters."""

    def count() -> TableCounts:
        """Read filtered graph counts and close the source afterward."""
        source = PostgreSQLGraphSource(
            config.paths,
            _reader_resources(config),
            replace(config.postgres, pool_size=1),
            LanguageSelector(load_selection_registry(config.registry_path)),
            IriPolicy(),
            snapshot_id=config.snapshot,
        )
        try:
            return source.export_table_counts()
        finally:
            source.close()

    return count


def _select_graph_run(
    graph_root: Path,
    observations: Observations,
    target: SnapshotTarget,
    preferred_registry: str,
) -> GraphArtifactEvidence | None:
    """Select completed graph evidence, preferring the requested registry."""
    if not graph_root.is_dir():
        return None
    parsed: dict[str, GraphArtifactEvidence] = {}

    def features(run: str) -> tuple[JSONObject, str | None]:
        """Cache a run's graph evidence and return its selection identity."""
        evidence = GraphArtifactAdapter(graph_root / run, run=run).evidence()
        parsed[run] = evidence
        return evidence.binding(), evidence.registry_digest

    selected = select_completed_run(
        graph_root,
        completed=graph_completed,
        features=features,
        observations=observations,
        artifact="graph",
        target=target,
        preferred_registry=preferred_registry,
    )
    return parsed.get(selected) if selected is not None else None


def build_finalization_validator(
    config: RunConfig,
    environ: Mapping[str, str],
    *,
    window_counts: PostgresWindowCounter | None = None,
    graph_export_counts: GraphExportCounts | None = None,
) -> FinalizationValidator:
    """
    Gate the finalized marker on the public `postgres-qdrant` check.

    The Qdrant evidence is the exact state the finalization readiness pass just
    established; no second readiness or count round-trip happens here. A
    completed graph counterpart joins as the independent, non-blocking `graph-
    qdrant` pairing; an absent counterpart is recorded as skipped. A selected
    counterpart without a prior validation for the pinned target is revalidated
    in the same invocation: its own non-blocking `graph-postgres` chain runs
    before the pairing, and its report becomes the durable record later
    invocations read. Only the blocking own-system check can stop finalization,
    and the run's committed artifacts and markers stay untouched either way.
    """
    stage_paths = embedded_validation_paths(config.paths.data_root, environ)
    window = window_counts if window_counts is not None else _postgres_window_counter(config)
    graph_counts = graph_export_counts if graph_export_counts is not None else _graph_export_counts(config)

    def validate(*, run: str, manifest: Manifest, report: JSONObject, points: int, qdrant: CollectionState) -> None:
        """Check finalized vector evidence and publish embedded validation results."""
        build_id, dense_dimension = qdrant.build_id, qdrant.dense_dimension
        if build_id is None or dense_dimension is None:
            raise RuntimeError("Finalized Qdrant collection does not report its build identity and dimension")
        target = SnapshotTarget(snapshot_id=manifest.snapshot_id, snapshot_digest=manifest.snapshot_digest)
        vector = vector_artifact_evidence(run, manifest, report, points)
        qdrant_evidence = QdrantCollectionEvidence(
            collection=qdrant.name,
            build_id=build_id,
            dense_dimension=dense_dimension,
            point_count=qdrant.point_count,
        )
        observations = parse_observations(load_reports(stage_paths.run_root))
        graph = _select_graph_run(stage_paths.graph_root, observations, target, manifest.registry_digest)
        vector_validated_now = False

        def postgres_qdrant() -> CheckResults:
            """Validate the source window and collection, recording success locally."""
            nonlocal vector_validated_now
            window_questions = window(
                snapshot_id=manifest.snapshot_id,
                after_id=manifest.after_id,
                limit=manifest.question_limit,
            )
            results = validate_postgres_qdrant(
                vector=vector, postgres_window_questions=window_questions, qdrant=qdrant_evidence
            )
            vector_validated_now = True
            return results

        checks: list[EmbeddedCheck | EmbeddedSkip] = [
            EmbeddedCheck(POSTGRES_QDRANT, blocking=True, execute=postgres_qdrant)
        ]
        artifacts = [ReportArtifact("vector", run, vector.binding(), (POSTGRES_QDRANT, GRAPH_QDRANT))]
        if graph is None:
            checks.append(EmbeddedSkip(GRAPH_QDRANT, "counterpart-unavailable"))
        else:
            graph_validated = validated_for_target(
                observations,
                artifact="graph",
                run=graph.run,
                binding=graph.binding(),
                target_snapshot_id=target.snapshot_id,
            )
            graph_revalidated_now = False

            def revalidate_graph() -> CheckResults:
                """Recheck graph counts and translate unavailable evidence into a mismatch."""
                nonlocal graph_revalidated_now
                try:
                    results = validate_graph_postgres(expected_counts=graph_counts(), graph=graph)
                except ValidationMismatchError:
                    raise
                except Exception as error:
                    LOGGER.warning("Graph counterpart evidence unavailable for revalidation: %s", error)
                    raise ValidationMismatchError(
                        GRAPH_POSTGRES,
                        CheckResults(
                            stage=GRAPH_POSTGRES,
                            comparisons=(
                                prerequisite_comparison("graph-postgres.counterpart-evidence", satisfied=False),
                            ),
                        ),
                    ) from error
                graph_revalidated_now = True
                return results

            if not graph_validated:
                checks.append(EmbeddedCheck(GRAPH_POSTGRES, blocking=False, execute=revalidate_graph))

            def graph_qdrant() -> CheckResults:
                """Compare graph and vector evidence using this invocation's validation state."""
                return validate_graph_qdrant(
                    graph=graph,
                    vector=vector,
                    qdrant=qdrant_evidence,
                    graph_validated_for_target=graph_validated or graph_revalidated_now,
                    vector_validated_for_target=vector_validated_now,
                )

            checks.append(EmbeddedCheck(GRAPH_QDRANT, blocking=False, execute=graph_qdrant))
            stages = (GRAPH_POSTGRES, GRAPH_QDRANT) if not graph_validated else (GRAPH_QDRANT,)
            artifacts.append(ReportArtifact("graph", graph.run, graph.binding(), stages))
        run_embedded_validation(
            run_root=stage_paths.run_root,
            command="vector",
            target=target,
            checks=checks,
            artifacts=artifacts,
        )

    return validate
