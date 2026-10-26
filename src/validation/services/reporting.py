"""
Record stage outcomes and build reports for validation orchestrators.

The standalone and embedded orchestrators own blocking decisions, locks, run
names, and source acquisition. This module records their results and builds the
report documents they publish.
"""

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter

from utils.types import JSONObject, JSONValue
from validation.adapter.reports import write_report
from validation.domain.models import SnapshotTarget, ValidationMismatchError
from validation.domain.report import (
    REPORT_VERSION,
    ArtifactKind,
    Observations,
    last_successful_reference,
    validated_for_target,
)
from validation.domain.results import CheckResults, Comparison, skipped_comparison
from validation.domain.stages import (
    GRAPH_POSTGRES,
    GRAPH_QDRANT,
    POSTGRES_QDRANT,
    STAGE_OWNERS,
    XML_POSTGRES,
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class StageRecord:
    name: str
    outcome: str
    seconds: float
    comparisons: tuple[Comparison, ...]
    details: JSONObject | None = None

    def to_json(self) -> JSONObject:
        """Serialize the stage outcome, duration, comparisons, and details."""
        stage: JSONObject = {
            "stage": self.name,
            "outcome": self.outcome,
            "seconds": round(self.seconds, 3),
            "comparisons": [item.to_json() for item in self.comparisons],
        }
        if self.details is not None:
            stage["details"] = self.details
        return stage


class StageRecorder:
    """Record check outcomes while allowing independent checks to continue."""

    def __init__(self) -> None:
        """Initialize an empty collection of stage results."""
        self.stages: dict[str, StageRecord] = {}

    def stage(self, name: str, call: Callable[[], CheckResults], details: JSONObject | None = None) -> bool:
        """Run and record a check, returning False for validation mismatches."""
        started = perf_counter()
        try:
            results = call()
        except ValidationMismatchError as error:
            elapsed = perf_counter() - started
            self.stages[name] = StageRecord(name, "failed", elapsed, error.results.comparisons, details)
            return False
        elapsed = perf_counter() - started
        self.stages[name] = StageRecord(name, "passed", elapsed, results.comparisons, details)
        return True

    def skip(self, name: str, detail: str) -> None:
        """Record a stage skipped because its prerequisite is unavailable."""
        self.stages[name] = StageRecord(
            name,
            "skipped",
            0.0,
            (skipped_comparison(name, reason="prerequisite-unavailable"),),
            {"skip-reason": detail},
        )

    def outcome_for(self, system: str, names: tuple[str, ...]) -> str:
        """
        Return the outcome attributable to a system.

        A system failed only on comparisons attributed to it or on unattributed
        comparisons in a stage whose subject is that system. Pairing stages (graph-
        qdrant) have no owner: a prerequisite fails only the counterpart it names,
        while configuration and envelope conflicts fail the run without rewriting
        either artifact's own validated state.
        """
        executed = [
            self.stages[name] for name in names if name in self.stages and self.stages[name].outcome != "skipped"
        ]
        if not executed:
            return "skipped"
        for stage in executed:
            owner = STAGE_OWNERS.get(stage.name)
            for item in stage.comparisons:
                if item.outcome == "failed" and (item.system == system or (item.system is None and owner == system)):
                    return "failed"
        return "passed"

    @property
    def failed(self) -> bool:
        """Whether any recorded stage failed."""
        return any(stage.outcome == "failed" for stage in self.stages.values())


@dataclass(frozen=True)
class ReportArtifact:
    """Record an artifact's validation outcome for later pairing checks."""

    kind: ArtifactKind
    run: str
    binding: JSONObject
    stages: tuple[str, ...]


def systems_summary(recorder: StageRecorder) -> JSONObject:
    """Return the validation outcome attributed to each system."""
    return {
        "postgres": recorder.outcome_for("postgres", (XML_POSTGRES,)),
        "vector": recorder.outcome_for("vector", (POSTGRES_QDRANT, GRAPH_QDRANT)),
        "graph": recorder.outcome_for("graph", (GRAPH_POSTGRES, GRAPH_QDRANT)),
        "qdrant": recorder.outcome_for("qdrant", (POSTGRES_QDRANT, GRAPH_QDRANT)),
    }


def success_report_document(
    *,
    validation_run: str,
    command: str,
    started_at: datetime,
    completed_at: datetime,
    total_seconds: float,
    target: SnapshotTarget,
    recorder: StageRecorder,
    observations: Observations,
    artifacts: Sequence[ReportArtifact],
) -> JSONObject:
    """
    Build a completed validation report for the caller to publish.

    Each artifact entry carries its own outcome and whether it was outdated
    before this invocation; the standalone orchestrator adds its "outdated"
    reason at its own serialization boundary.
    """
    systems = systems_summary(recorder)
    artifact_documents: list[JSONValue] = []
    references: JSONObject = {}
    for artifact in artifacts:
        outcome = recorder.outcome_for(artifact.kind, artifact.stages)
        if outcome not in ("passed", "failed"):
            continue
        initially_outdated = not validated_for_target(
            observations,
            artifact=artifact.kind,
            run=artifact.run,
            binding=artifact.binding,
            target_snapshot_id=target.snapshot_id,
        )
        artifact_documents.append(
            {
                "artifact": artifact.kind,
                "run": artifact.run,
                "binding": artifact.binding,
                "outcome": outcome,
                "initially_outdated": initially_outdated,
            }
        )
        reference = last_successful_reference(
            observations,
            artifact=artifact.kind,
            run=artifact.run,
            binding=artifact.binding,
            target_snapshot_id=target.snapshot_id,
        )
        if reference is not None:
            references[artifact.kind] = reference
    return {
        "version": REPORT_VERSION,
        "validation_run": validation_run,
        "command": command,
        "started_at": started_at.isoformat(timespec="seconds"),
        "completed_at": completed_at.isoformat(timespec="seconds"),
        "total_seconds": total_seconds,
        "outcome": "failed" if recorder.failed else "passed",
        "target": target.to_json(),
        "systems": systems,
        "artifacts": artifact_documents,
        "successful_validations": references,
        "stages": [stage.to_json() for stage in recorder.stages.values()],
    }


def failure_report_document(
    *,
    validation_run: str,
    command: str,
    started_at: datetime,
    completed_at: datetime,
    total_seconds: float,
    target: SnapshotTarget | None,
    error: Exception,
) -> JSONObject:
    """Build a crash report for a standalone run without a pinned target."""
    return {
        "version": REPORT_VERSION,
        "validation_run": validation_run,
        "command": command,
        "started_at": started_at.isoformat(timespec="seconds"),
        "completed_at": completed_at.isoformat(timespec="seconds"),
        "total_seconds": total_seconds,
        "outcome": "failed",
        "target": target.to_json() if target is not None else None,
        "systems": {},
        "artifacts": [],
        "successful_validations": {},
        "stages": [],
        "error": f"{type(error).__name__}: {str(error)[:500]}",
    }


def publish_failure(directory: Path, document: JSONObject, *, unavailable_notice: str) -> None:
    """Write a failure report and log a warning if writing fails."""
    try:
        write_report(directory, document)
    except OSError as unavailable:
        LOGGER.warning(unavailable_notice, unavailable)
