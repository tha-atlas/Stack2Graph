"""Check collection readiness and point counts before writing the report."""

import time
from collections.abc import Callable, Mapping
from typing import Protocol

from utils.artifacts import atomic_write_json
from utils.types import JSONObject
from vector.domain.models import CollectionState
from vector.shards import (
    ChunkMarker,
    ClaimStage,
    Manifest,
    RunLayout,
    StageMarker,
    json_number,
    read_json_file,
    shard_name,
)


class VerifyingStore(Protocol):
    def verify(self, expected_points: int, *, deadline: float) -> CollectionState:
        """Restore collection readiness and verify its count before the deadline."""
        ...


class FinalizationValidator(Protocol):
    """
    Validate a build before publishing its final report and marker.

    The caller supplies the exact Qdrant state from the fresh readiness check,
    along with the run manifest and report. This gate does not repeat that
    check. Any exception, normally `ValidationMismatchError`, leaves the report
    and marker unwritten. Committed stage artifacts are preserved.
    """

    def __call__(
        self, *, run: str, manifest: Manifest, report: JSONObject, points: int, qdrant: CollectionState
    ) -> None:
        """Validate the completed build before publishing finalization markers."""
        ...


def expected_points(layout: RunLayout) -> int:
    """Sum confirmed loaded rows, rejecting incomplete extraction or loading."""
    if not layout.extract_complete.exists():
        raise RuntimeError("Extraction is not complete")
    shards = layout.finished_chunk_shards()
    missing = [shard for shard in shards if not layout.loaded_done(shard).exists()]
    if missing:
        raise RuntimeError(f"Shards not loaded yet: {', '.join(shard_name(shard) for shard in missing[:10])}")
    return sum(StageMarker.from_json(read_json_file(layout.loaded_done(shard))).rows for shard in shards)


def _stage_seconds(layout: RunLayout, stage: ClaimStage) -> float:
    """Sum recorded worker seconds for a stage across completed shards."""
    total = 0.0
    for shard in layout.finished_chunk_shards():
        path = layout.stage_done(stage, shard)
        if path.exists():
            total += StageMarker.from_json(read_json_file(path)).seconds
    return total


def _model_load_seconds(layout: RunLayout) -> float:
    """Return the longest recorded GPU model load time, or zero."""
    seconds = [json_number(read_json_file(path)["load_seconds"]) for path in layout.embeddings.glob("worker-*.json")]
    return max(seconds, default=0.0)


def build_report(layout: RunLayout, stage_seconds: Mapping[str, float], settings: JSONObject) -> JSONObject:
    """Summarize shard counts, stage timings, throughput, and run settings."""
    markers = [ChunkMarker.from_json(read_json_file(layout.chunk_done(s))) for s in layout.finished_chunk_shards()]
    points = sum(marker.rows for marker in markers)
    model_load = _model_load_seconds(layout)
    wall: JSONObject = {}
    for name, seconds in stage_seconds.items():
        wall[name] = seconds
    total = stage_seconds.get("all")
    report: JSONObject = {
        "run": layout.root.name,
        "points": points,
        "scanned_questions": sum(marker.scanned for marker in markers),
        "not_selected": sum(marker.not_selected for marker in markers),
        "chunking_errors": sum(marker.chunking_errors for marker in markers),
        "shards": len(markers),
        "wall_seconds": wall,
        "model_load_seconds": model_load,
        "points_per_second": points / (total - model_load) if total is not None and total > model_load else None,
        "dense_worker_seconds": _stage_seconds(layout, "dense"),
        "sparse_worker_seconds": _stage_seconds(layout, "sparse"),
        "load_seconds": _stage_seconds(layout, "load"),
        "settings": settings,
    }
    return report


def run_finalize(
    layout: RunLayout,
    store: VerifyingStore,
    timeout_seconds: int,
    stage_seconds: Mapping[str, float],
    settings: JSONObject,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    started: float | None = None,
    manifest: Manifest | None = None,
    validation: FinalizationValidator | None = None,
) -> JSONObject:
    """
    Verify collection readiness and publish the final report once.

    `started` is the `monotonic()` time the `all` run began; its wall time then
    includes finalization. When `validation` is given, the finalized marker and
    report appear only after it passes.

    Args:
        layout: Run files and completion markers.
        store: Collection adapter that checks readiness and exact point count.
        timeout_seconds: Maximum finalization duration.
        stage_seconds: Recorded stage wall times for the report.
        settings: Run settings to include in the report.
        monotonic: Clock used to enforce the finalization deadline.
        started: All-stage start on the same clock, including finalization time.
        manifest: Build identity passed to optional embedded validation.
        validation: Optional check that must pass before publication.

    Returns:
        Final report after readiness and any supplied validation pass.

    """
    if layout.finalized.exists():
        return read_json_file(layout.report)
    expected = expected_points(layout)
    state = store.verify(expected, deadline=monotonic() + timeout_seconds)
    seconds = dict(stage_seconds)
    if started is not None:
        seconds["all"] = monotonic() - started
    report = build_report(layout, seconds, settings)
    if validation is not None:
        if manifest is None:
            raise RuntimeError("Finalization validation requires the run manifest")
        validation(run=layout.root.name, manifest=manifest, report=report, points=expected, qdrant=state)
    atomic_write_json(layout.report, report)
    atomic_write_json(layout.finalized, {"points": expected})
    return report
