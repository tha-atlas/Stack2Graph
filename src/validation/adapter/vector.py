"""Read finalized vector metadata without opening shard contents."""

from pathlib import Path

from utils.types import JSONObject, json_integer
from validation.domain.models import VectorArtifactEvidence
from vector.shards import Manifest, RunLayout, read_json_file


def vector_completed(run: Path) -> Path | None:
    """Return a finalized vector run's completion marker path, or None."""
    layout = RunLayout(run)
    if layout.finalized.is_file() and layout.extract_complete.is_file() and layout.report.is_file():
        return layout.finalized
    return None


def vector_artifact_evidence(run: str, manifest: Manifest, report: JSONObject, points: int) -> VectorArtifactEvidence:
    """Map manifest and report evidence; callers check run completion."""
    scanned = json_integer(report["scanned_questions"])
    selected = scanned - json_integer(report["not_selected"]) - json_integer(report["chunking_errors"])
    return VectorArtifactEvidence(
        run=run,
        source_snapshot_id=manifest.snapshot_id,
        source_snapshot_digest=manifest.snapshot_digest,
        registry_digest=manifest.registry_digest,
        collection=manifest.collection,
        build_id=run,
        dense_dimension=manifest.dense_dimension,
        scanned_questions=scanned,
        finalized_points=points,
        selected_questions=selected,
        after_id=manifest.after_id,
        question_limit=manifest.question_limit,
    )


class VectorArtifactAdapter:
    """Read a run's manifest, report, and finalization marker without shards."""

    def __init__(self, layout: RunLayout, *, run: str) -> None:
        """Store the vector run layout and name for evidence reads."""
        self._layout = layout
        self._run = run

    def evidence(self) -> VectorArtifactEvidence:
        """Read finalized vector evidence and reject conflicting point counts."""
        layout = self._layout
        if not layout.finalized.is_file():
            raise ValueError(f"Vector run {self._run} is not finalized")
        if not layout.extract_complete.is_file() or not layout.report.is_file():
            raise ValueError(f"Vector run {self._run} lacks finalization metadata")
        manifest = Manifest.from_json(read_json_file(layout.manifest))
        report = read_json_file(layout.report)
        finalized = read_json_file(layout.finalized)
        points = json_integer(report["points"])
        if json_integer(finalized["points"]) != points:
            raise ValueError(f"Vector run {self._run} reports {points} points but finalized marker disagrees")
        return vector_artifact_evidence(self._run, manifest, report, points)
