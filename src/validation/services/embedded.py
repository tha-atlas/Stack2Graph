"""
Run public count checks inside a pipeline and publish one report.

Standalone validation resolves completed artifacts. An in-flight pipeline has
no completion marker yet, so it supplies checks directly to this executor. Both
paths reuse report storage, observation helpers, and attribution rules. A
system fails on comparisons attributed to it or on unattributed comparisons in
a stage it owns. Each invocation writes one report to
`<run_root>/<name>/report.json`.
"""

import logging
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Literal
from uuid import uuid4

from utils.config import run_root
from utils.logging_utils import pipeline_run
from utils.manifest import FileLock
from utils.types import JSONObject
from validation.adapter.reports import load_reports, write_report
from validation.domain.models import SnapshotTarget, ValidationMismatchError
from validation.domain.report import parse_observations
from validation.domain.results import CheckResults, Comparison
from validation.services.reporting import (
    ReportArtifact,
    StageRecorder,
    failure_report_document,
    publish_failure,
    success_report_document,
)
from validation.services.runs import create_run_directory

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class EmbeddedCheck:
    """Run one public validation check from a processing pipeline."""

    name: str
    blocking: bool
    execute: Callable[[], CheckResults]
    details: JSONObject | None = None


@dataclass(frozen=True, slots=True)
class EmbeddedSkip:
    """Record a missing counterpart as skipped for the target, without failure."""

    name: str
    reason: str


@dataclass(frozen=True, slots=True)
class EmbeddedValidationOutcome:
    validation_run: str
    outcome: Literal["passed", "failed"]
    stage_outcomes: dict[str, str]
    document: JSONObject


@dataclass(frozen=True, slots=True)
class EmbeddedStagePaths:
    """Keep validated run roots with the standalone validation defaults."""

    run_root: Path
    graph_root: Path
    vector_root: Path


def embedded_validation_paths(data_root: Path, environ: Mapping[str, str]) -> EmbeddedStagePaths:
    """Resolve graph, vector, and validation roots from shared environment keys."""
    return EmbeddedStagePaths(
        run_root(environ, data_root, "VALIDATION"),
        run_root(environ, data_root, "GRAPH"),
        run_root(environ, data_root, "VECTOR"),
    )


def run_embedded_validation(
    *,
    run_root: Path,
    command: str,
    target: SnapshotTarget,
    checks: Sequence[EmbeddedCheck | EmbeddedSkip],
    artifacts: Sequence[ReportArtifact] = (),
    clock: Callable[[], datetime] | None = None,
) -> EmbeddedValidationOutcome:
    """
    Run pinned checks and publish one validation report.

    Every check runs even after an earlier one failed, and the report is written
    for failed and crashed invocations alike. A check marked blocking raises
    `ValidationMismatchError` only after the report exists, so the pipeline gate
    can refuse publication while the durable record of every comparison stays
    available.

    Args:
        run_root: Parent directory for the new validation run and report.
        command: Pipeline name recorded in the report.
        target: Snapshot identity pinned by the calling pipeline.
        checks: Checks and skips to record; all checks run despite mismatches.
        artifacts: Artifact bindings and outcome callbacks for the report.
        clock: Optional aware-datetime clock for report timestamps.

    Returns:
        The invocation outcome after its report has been written.

    Raises:
        ValidationMismatchError: A blocking check fails; the report already exists.

    """
    moment: Callable[[], datetime] = clock if clock is not None else (lambda: datetime.now(UTC))
    started_at = moment()
    started = perf_counter()
    with pipeline_run("validation", log_dir=run_root, reuse_active=True):
        name = f"validation-{started_at.strftime('%Y%m%dT%H%M%S')}-{os.getpid()}-{uuid4().hex[:8]}"
        directory = create_run_directory(run_root, name)
        with FileLock(directory / "validation.lock", f"Validation run {name!r} is already active"):
            recorder = StageRecorder()
            blocking_failure: tuple[str, tuple[Comparison, ...]] | None = None
            try:
                observations = parse_observations(load_reports(run_root))
                for item in checks:
                    if isinstance(item, EmbeddedSkip):
                        recorder.skip(item.name, item.reason)
                        continue
                    passed = recorder.stage(item.name, item.execute, item.details)
                    if not passed and item.blocking and blocking_failure is None:
                        blocking_failure = (item.name, recorder.stages[item.name].comparisons)
            except Exception as error:
                completed_at = moment()
                publish_failure(
                    directory,
                    failure_report_document(
                        validation_run=name,
                        command=command,
                        started_at=started_at,
                        completed_at=completed_at,
                        total_seconds=round(perf_counter() - started, 3),
                        target=target,
                        error=error,
                    ),
                    unavailable_notice="embedded validation failure report unavailable: %s",
                )
                raise
            completed_at = moment()
            document = success_report_document(
                validation_run=name,
                command=command,
                started_at=started_at,
                completed_at=completed_at,
                total_seconds=round(perf_counter() - started, 3),
                target=target,
                recorder=recorder,
                observations=observations,
                artifacts=artifacts,
            )
            write_report(directory, document)
            if blocking_failure is not None:
                stage, comparisons = blocking_failure
                raise ValidationMismatchError(stage, CheckResults(stage=stage, comparisons=comparisons))
            outcome: Literal["passed", "failed"] = "failed" if recorder.failed else "passed"
            return EmbeddedValidationOutcome(
                validation_run=name,
                outcome=outcome,
                stage_outcomes={stage_name: stage.outcome for stage_name, stage in recorder.stages.items()},
                document=document,
            )
