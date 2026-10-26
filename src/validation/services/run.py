"""Resolve a validation target, run its checks, and publish the report."""

import logging
import os
import time
from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack, closing
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from sql.adapter.reader import PostgreSQLSnapshotReader
from utils.logging_utils import pipeline_run
from utils.manifest import FileLock
from utils.resources import ResourceCaps, ResourcePlan, detect_limits, plan_resources
from utils.types import JSONObject, json_array, json_object
from validation.adapter.graph import GraphArtifactAdapter, graph_completed
from validation.adapter.postgres import PostgreSQLCountAdapter
from validation.adapter.qdrant import QdrantCollectionAdapter
from validation.adapter.reports import load_reports, write_report
from validation.adapter.vector import VectorArtifactAdapter, vector_completed
from validation.config import ValidationConfig
from validation.domain.models import (
    GraphArtifactEvidence,
    QdrantCollectionEvidence,
    SnapshotTarget,
    ValidationRequest,
    VectorArtifactEvidence,
)
from validation.domain.report import (
    Observations,
    parse_observations,
    reusable_expected_counts,
    validated_for_target,
)
from validation.domain.stages import GRAPH_POSTGRES, GRAPH_QDRANT, POSTGRES_QDRANT, XML_POSTGRES
from validation.interfaces.sources import (
    GraphArtifactSource,
    GraphExportCountSource,
    PostgreSQLCountSource,
    QdrantCollectionSource,
    VectorArtifactSource,
)
from validation.services.checks import (
    validate_graph_postgres,
    validate_graph_qdrant,
    validate_postgres_qdrant,
    validate_xml_postgres,
)
from validation.services.counterparts import select_completed_run
from validation.services.reporting import (
    ReportArtifact,
    StageRecorder,
    failure_report_document,
    publish_failure,
    success_report_document,
)
from validation.services.runs import create_run_directory
from vector.shards import RunLayout

LOGGER = logging.getLogger(__name__)
EXPORT_COUNT_CONNECTIONS = 64

ValidationScope = Literal["postgres", "vector", "graph", "run"]
PostgresContext = AbstractContextManager[PostgreSQLCountSource]
QdrantContext = AbstractContextManager[QdrantCollectionSource]
ExportCountsContext = AbstractContextManager[GraphExportCountSource]


@dataclass(frozen=True)
class ValidationFactories:
    """Supply constructors for the external sources used by a validation run."""

    postgres: Callable[[ValidationConfig, str | None], PostgresContext]
    graph: Callable[[ValidationConfig, str], GraphArtifactSource]
    vector: Callable[[ValidationConfig, str], VectorArtifactSource]
    qdrant: Callable[[ValidationConfig], QdrantContext]
    export_counts: Callable[[ValidationConfig, str], ExportCountsContext]


def _reader_resources(config: ValidationConfig) -> ResourcePlan:
    """Plan scalable reader resources from validation row and byte caps."""
    limits = config.limits
    return plan_resources(
        detect_limits(),
        ResourceCaps(
            batch_rows=limits.read_rows,
            batch_bytes=limits.read_bytes,
            record_bytes=limits.read_bytes,
        ),
        scalable=True,
    )


def default_factories() -> ValidationFactories:
    """Create production adapters when a validation run requests them."""

    def postgres(config: ValidationConfig, snapshot_id: str | None) -> PostgresContext:
        """Open a pinned count adapter in a context that closes it on exit."""
        reader = PostgreSQLSnapshotReader(
            config.paths,
            _reader_resources(config),
            config.postgres,
            snapshot_id=snapshot_id,
        )
        return closing(PostgreSQLCountAdapter(reader))

    def graph(config: ValidationConfig, run: str) -> GraphArtifactSource:
        """Create an evidence reader for the named graph run."""
        return GraphArtifactAdapter(config.graph_root / run, run=run)

    def vector(config: ValidationConfig, run: str) -> VectorArtifactSource:
        """Create an evidence reader for the named vector run."""
        return VectorArtifactAdapter(RunLayout(config.vector_root / run), run=run)

    def qdrant(config: ValidationConfig) -> QdrantContext:
        """Create a collection adapter in a context that closes it on exit."""
        return closing(QdrantCollectionAdapter(config.qdrant))

    def export_counts(config: ValidationConfig, snapshot_id: str) -> ExportCountsContext:
        """Open a pinned graph source for filtered export-count checks."""
        from graph.adapter.postgres_source import PostgreSQLGraphSource
        from graph.mapping.identity import IriPolicy
        from selection.languages import LanguageSelector
        from selection.registry import load_selection_registry

        source = PostgreSQLGraphSource(
            config.paths,
            _reader_resources(config),
            replace(config.postgres, pool_size=max(config.postgres.pool_size, EXPORT_COUNT_CONNECTIONS)),
            LanguageSelector(load_selection_registry(config.registry_path)),
            IriPolicy(),
            snapshot_id=snapshot_id,
        )
        return closing(source)

    return ValidationFactories(postgres, graph, vector, qdrant, export_counts)


@dataclass(frozen=True)
class _RunPlan:
    name: str
    directory: Path


def run_validation(
    config: ValidationConfig,
    request: ValidationRequest,
    scope: ValidationScope,
    *,
    factories: ValidationFactories | None = None,
    clock: Callable[[], datetime] | None = None,
) -> JSONObject:
    """
    Run locked, pinned validation and publish one report.

    Failed count comparisons return a report with outcome failed; only
    configuration or resolution errors raise. Both paths publish their report.

    Args:
        config: Snapshot, artifact roots, collection, and read limits.
        request: Optional pinned run names and snapshot selection.
        scope: Validation command selecting the checks to run.
        factories: Adapter factories; None uses the production adapters.
        clock: Optional aware-datetime clock for report timestamps.

    Returns:
        The published report, including failed count-comparison outcomes.

    Raises:
        Exception: Configuration or resolution fails after publishing a crash report.

    """
    active = factories if factories is not None else default_factories()
    moment: Callable[[], datetime] = clock if clock is not None else (lambda: datetime.now(UTC))
    started_at = moment()
    started = time.perf_counter()
    with pipeline_run("validation", log_dir=config.run_root, reuse_active=True):
        plan = _resolve_plan(config, request, moment())
        with FileLock(plan.directory / "validation.lock", f"Validation run {plan.name} is already active"):
            try:
                return _execute(config, request, scope, active, plan, started, started_at, moment)
            except Exception as error:
                completed_at = moment()
                publish_failure(
                    plan.directory,
                    failure_report_document(
                        validation_run=plan.name,
                        command=scope,
                        started_at=started_at,
                        completed_at=completed_at,
                        total_seconds=round(time.perf_counter() - started, 3),
                        target=None,
                        error=error,
                    ),
                    unavailable_notice="validation failure report unavailable: %s",
                )
                raise


def _resolve_plan(config: ValidationConfig, request: ValidationRequest, moment: datetime) -> _RunPlan:
    """Claim a fresh run directory using the requested or generated name."""
    name = request.run or f"validation-{moment.strftime('%Y%m%dT%H%M%S')}-{os.getpid()}"
    return _RunPlan(name, create_run_directory(config.run_root, name))


def _execute(
    config: ValidationConfig,
    request: ValidationRequest,
    scope: ValidationScope,
    factories: ValidationFactories,
    plan: _RunPlan,
    started: float,
    started_at: datetime,
    moment: Callable[[], datetime],
) -> JSONObject:
    """Resolve artifact evidence, run scoped checks, and publish the report."""
    observations = parse_observations(load_reports(config.run_root))
    recorder = StageRecorder()
    with ExitStack() as stack:
        postgres = stack.enter_context(factories.postgres(config, request.snapshot))
        target = postgres.snapshot_target

        vector_evidence: VectorArtifactEvidence | None = None
        vector_run: str | None = None
        graph_evidence: GraphArtifactEvidence | None = None
        graph_run: str | None = None
        qdrant_evidence: QdrantCollectionEvidence | None = None

        if scope in ("postgres", "run"):
            counts = postgres.table_counts()
            recorder.stage(
                XML_POSTGRES,
                lambda: validate_xml_postgres(xml_counts=counts.xml_counts, postgres_counts=counts.measured_counts),
                {"count-source": counts.source},
            )

        # One vector/Qdrant counterpart acquisition for every non-postgres scope:
        # blocking for vector and run, optional for graph.
        if scope != "postgres":
            vector_run = _resolve_vector_run(
                config, request, observations, target, factories, required=scope in ("vector", "run")
            )
            if vector_run is not None:
                vector_evidence = factories.vector(config, vector_run).evidence()
                qdrant_evidence = stack.enter_context(factories.qdrant(config)).evidence()
        if scope in ("vector", "run"):
            if vector_evidence is None:
                raise ValueError(f"No completed vector run found under {config.vector_root}")
            if qdrant_evidence is None:
                raise ValueError(
                    f"{scope} validation requires the live Qdrant collection "
                    f"{config.qdrant.collection!r}; it does not exist"
                )

        graph_postgres_passed = False
        if scope in ("graph", "run"):
            graph_run = _resolve_graph_run(
                config, request, observations, target, vector_evidence, factories, required=True
            )
            if graph_run is None:
                raise ValueError(f"No completed graph run found under {config.graph_root}")
            graph_document = factories.graph(config, graph_run).evidence()
            graph_evidence = graph_document
            expected = reusable_expected_counts(
                observations, run=graph_run, binding=graph_evidence.binding(), target=target
            )
            if expected is None:
                export = stack.enter_context(factories.export_counts(config, target.snapshot_id))
                expected = export.export_table_counts()
                expected_source = "fresh-scan"
            else:
                expected_source = "trusted-counts"
            graph_postgres_passed = recorder.stage(
                GRAPH_POSTGRES,
                lambda: validate_graph_postgres(expected_counts=expected, graph=graph_document),
                {"expected-count-source": expected_source},
            )

        postgres_qdrant_passed = False
        if vector_evidence is not None and qdrant_evidence is not None:
            window = postgres.window_question_count(
                after_id=vector_evidence.after_id, limit=vector_evidence.question_limit
            )
            postgres_qdrant_passed = recorder.stage(
                POSTGRES_QDRANT,
                lambda: validate_postgres_qdrant(
                    vector=vector_evidence, postgres_window_questions=window, qdrant=qdrant_evidence
                ),
            )

        graph_validated = graph_postgres_passed
        vector_validated = postgres_qdrant_passed

        if scope == "vector":
            graph_run = _resolve_graph_run(
                config, request, observations, target, vector_evidence, factories, required=False
            )
            if graph_run is not None:
                counterpart = factories.graph(config, graph_run).evidence()
                graph_evidence = counterpart
                graph_validated = validated_for_target(
                    observations,
                    artifact="graph",
                    run=counterpart.run,
                    binding=counterpart.binding(),
                    target_snapshot_id=target.snapshot_id,
                )
                if not graph_validated:
                    # The counterpart lacks a validation for the pinned target, so run
                    # its own blocking chain once for this invocation (expected counts
                    # through the shared graph-adapter export filters). A failure may
                    # only fail the pairing and its attribution, never this vector scope.
                    export = stack.enter_context(factories.export_counts(config, target.snapshot_id))
                    counterpart_expected = export.export_table_counts()
                    graph_validated = recorder.stage(
                        GRAPH_POSTGRES,
                        lambda: validate_graph_postgres(expected_counts=counterpart_expected, graph=counterpart),
                        {"expected-count-source": "fresh-scan", "revalidation": "counterpart"},
                    )
        if scope == "graph" and vector_evidence is not None:
            # The vector counterpart's own chain ran above when live evidence existed;
            # its pairing prerequisite is that result or a prior validation for the target.
            vector_validated = postgres_qdrant_passed or validated_for_target(
                observations,
                artifact="vector",
                run=vector_evidence.run,
                binding=vector_evidence.binding(),
                target_snapshot_id=target.snapshot_id,
            )

        _run_envelope(
            scope,
            recorder,
            graph_evidence=graph_evidence,
            vector_evidence=vector_evidence,
            qdrant_evidence=qdrant_evidence,
            graph_validated=graph_validated,
            vector_validated=vector_validated,
        )

        return _publish_success(
            plan=plan,
            scope=scope,
            started=started,
            started_at=started_at,
            moment=moment,
            target=target,
            recorder=recorder,
            observations=observations,
            vector_run=vector_run,
            vector_evidence=vector_evidence,
            graph_run=graph_run,
            graph_evidence=graph_evidence,
        )


def _publish_success(
    *,
    plan: _RunPlan,
    scope: ValidationScope,
    started: float,
    started_at: datetime,
    moment: Callable[[], datetime],
    target: SnapshotTarget,
    recorder: StageRecorder,
    observations: Observations,
    vector_run: str | None,
    vector_evidence: VectorArtifactEvidence | None,
    graph_run: str | None,
    graph_evidence: GraphArtifactEvidence | None,
) -> JSONObject:
    """Serialize and publish the standalone invocation's single report."""
    artifacts: list[ReportArtifact] = []
    if vector_run is not None and vector_evidence is not None:
        artifacts.append(
            ReportArtifact("vector", vector_run, vector_evidence.binding(), (POSTGRES_QDRANT, GRAPH_QDRANT))
        )
    if graph_run is not None and graph_evidence is not None:
        artifacts.append(ReportArtifact("graph", graph_run, graph_evidence.binding(), (GRAPH_POSTGRES, GRAPH_QDRANT)))
    document = success_report_document(
        validation_run=plan.name,
        command=scope,
        started_at=started_at,
        completed_at=moment(),
        total_seconds=round(time.perf_counter() - started, 3),
        target=target,
        recorder=recorder,
        observations=observations,
        artifacts=artifacts,
    )
    for raw_entry in json_array(document["artifacts"]):
        # Standalone schema: a stale artifact that is not successfully revalidated
        # stays failed with reason "outdated"; successful revalidation yields passed.
        entry = json_object(raw_entry)
        entry["reason"] = "outdated" if entry["initially_outdated"] is True and entry["outcome"] == "failed" else None
    write_report(plan.directory, document)
    return document


def _resolve_vector_run(
    config: ValidationConfig,
    request: ValidationRequest,
    observations: Observations,
    target: SnapshotTarget,
    factories: ValidationFactories,
    *,
    required: bool,
) -> str | None:
    """Select a completed vector run or validate the explicitly pinned run."""
    if request.vector_run is not None:
        if vector_completed(config.vector_root / request.vector_run) is None:
            raise ValueError(
                f"{'Required' if required else 'Pinned'} vector run {request.vector_run!r} "
                f"is not a completed run under {config.vector_root}"
            )
        return request.vector_run
    return select_completed_run(
        config.vector_root,
        completed=vector_completed,
        features=lambda name: _vector_features(factories, config, name),
        observations=observations,
        artifact="vector",
        target=target,
        preferred_registry=None,
    )


def _resolve_graph_run(
    config: ValidationConfig,
    request: ValidationRequest,
    observations: Observations,
    target: SnapshotTarget,
    vector_evidence: VectorArtifactEvidence | None,
    factories: ValidationFactories,
    *,
    required: bool,
) -> str | None:
    """Select a completed graph run, preferring a compatible vector registry."""
    preferred = vector_evidence.registry_digest if vector_evidence is not None else None
    if request.graph_run is not None:
        if graph_completed(config.graph_root / request.graph_run) is None:
            raise ValueError(
                f"{'Required' if required else 'Pinned'} graph run {request.graph_run!r} "
                f"is not a completed run under {config.graph_root}"
            )
        return request.graph_run
    return select_completed_run(
        config.graph_root,
        completed=graph_completed,
        features=lambda name: _graph_features(factories, config, name),
        observations=observations,
        artifact="graph",
        target=target,
        preferred_registry=preferred,
    )


def _vector_features(
    factories: ValidationFactories, config: ValidationConfig, run: str
) -> tuple[JSONObject, str | None]:
    """Read a vector run's identity binding and registry digest."""
    evidence = factories.vector(config, run).evidence()
    return evidence.binding(), evidence.registry_digest


def _graph_features(
    factories: ValidationFactories, config: ValidationConfig, run: str
) -> tuple[JSONObject, str | None]:
    """Read a graph run's identity binding and registry digest."""
    evidence = factories.graph(config, run).evidence()
    return evidence.binding(), evidence.registry_digest


def _run_envelope(
    scope: ValidationScope,
    recorder: StageRecorder,
    *,
    graph_evidence: GraphArtifactEvidence | None,
    vector_evidence: VectorArtifactEvidence | None,
    qdrant_evidence: QdrantCollectionEvidence | None,
    graph_validated: bool,
    vector_validated: bool,
) -> None:
    """
    Compose the optional graph/Qdrant envelope after the scope's own checks.

    Callers resolve both validation flags as "prior report validated for the
    target OR the counterpart chain passed in this invocation".
    """
    if scope == "postgres":
        return
    if graph_evidence is None or vector_evidence is None or qdrant_evidence is None:
        if scope == "run":
            raise ValueError(f"{scope} validation requires graph, vector, and Qdrant counterparts")
        recorder.skip(GRAPH_QDRANT, "counterpart-unavailable")
        return
    recorder.stage(
        GRAPH_QDRANT,
        lambda: validate_graph_qdrant(
            graph=graph_evidence,
            vector=vector_evidence,
            qdrant=qdrant_evidence,
            graph_validated_for_target=graph_validated,
            vector_validated_for_target=vector_validated,
        ),
    )
