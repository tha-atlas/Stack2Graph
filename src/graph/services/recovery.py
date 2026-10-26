"""Verify committed ranges and recover unfinished work under a writer lock."""

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

from graph.adapter.inventory import iter_shards, verify_file
from graph.domain.artifacts import ArtifactLayout, FileRef, RangeMarker, ShardCatalogRef
from graph.domain.config import RunIdentity
from graph.domain.source import QuestionRange
from utils.artifacts import AtomicBinaryWriter
from utils.manifest import SnapshotLock
from utils.types import json_integer, json_object, parse_json


@dataclass(frozen=True)
class RecoveryPlan:
    pending_catalog: Path
    shared_complete: bool
    completed_questions: int

    def iter_pending(self) -> Iterator[QuestionRange]:
        """Yield pending half-open question ranges from the recovery catalog."""
        with self.pending_catalog.open() as stream:
            for line in stream:
                value = json_object(parse_json(line))
                yield QuestionRange(json_integer(value["first_id"]), json_integer(value["end_id"]))


def validate_marker(layout: ArtifactLayout, marker: RangeMarker) -> None:
    """Verify a completed range's counts, file checksums, and shard ownership."""
    if marker.question_count < 0:
        raise ValueError("Negative committed question count")
    verify_file(layout, marker.usage)
    verify_file(layout, marker.membership)
    for shard in iter_shards(layout, marker.shard_catalog):
        if shard.part_key != marker.part.key:
            raise ValueError("Range catalog contains another partition")
        verify_file(layout, FileRef(shard.path, shard.stored_sha256, shard.bytes))


def clear_incomplete(layout: ArtifactLayout, key: str) -> None:
    """Remove one range's incomplete artifacts and temporary files."""
    for path in layout.root.glob(f"graphs/*/{key}-*.nq*"):
        layout.path(path.relative_to(layout.root).as_posix()).unlink()
    for directory in ("graphs", "catalogs", "usage", "membership"):
        for path in (layout.root / directory).rglob(f".{key}*.tmp"):
            layout.path(path.relative_to(layout.root).as_posix()).unlink()
    for relative in (f"catalogs/{key}.jsonl", f"usage/{key}.jsonl", f"membership/{key}.jsonl"):
        layout.path(relative).unlink(missing_ok=True)


def _recover(
    layout: ArtifactLayout,
    expected: RunIdentity,
    progress: Callable[[int], None] | None = None,
) -> RecoveryPlan:
    """Verify committed checkpoints and record pending ranges for a matching run."""
    build = json_object(parse_json(layout.path("build.json").read_text()))
    actual = RunIdentity.from_json(json_object(build["identity"]))
    if actual != expected:
        raise ValueError("Resume output identity is incompatible with existing build")
    pending = AtomicBinaryWriter(layout.path("pending.jsonl"))
    completed_questions = 0
    try:
        first, last = build["first_question"], build["last_question"]
        if first is not None and last is not None:
            first_id, last_id = json_integer(first), json_integer(last)
            for start in range(first_id, last_id + 1, expected.range_width):
                part = QuestionRange(start, min(start + expected.range_width, last_id + 1))
                path = layout.range_marker(part.key)
                if path.exists():
                    marker = RangeMarker.from_json(json_object(parse_json(path.read_text())))
                    if marker.part != part:
                        raise ValueError("Committed range boundaries differ from pinned partition")
                    validate_marker(layout, marker)
                    completed_questions += marker.question_count
                    if progress is not None:
                        progress(completed_questions)
                else:
                    clear_incomplete(layout, part.key)
                    partition_record: dict[str, int] = {"first_id": part.first_id, "end_id": part.end_id}
                    pending.stream.write((json.dumps(partition_record, separators=(",", ":")) + "\n").encode())
        pending.publish()
    finally:
        pending.discard()
    shared_complete = layout.path("shared.json").exists()
    for key in ("shared", "schema"):
        path = layout.path(f"{key}.json")
        if path.exists():
            value = json_object(parse_json(path.read_text()))
            catalog = ShardCatalogRef.from_json(json_object(value["catalog"]))
            for shard in iter_shards(layout, catalog):
                verify_file(layout, FileRef(shard.path, shard.stored_sha256, shard.bytes))
        else:
            clear_incomplete(layout, key)
    return RecoveryPlan(pending.path, shared_complete, completed_questions)


def recover(
    layout: ArtifactLayout,
    expected: RunIdentity,
    *,
    lock: SnapshotLock | None = None,
    progress: Callable[[int], None] | None = None,
) -> RecoveryPlan:
    """Recover a run under the supplied active lock or a newly acquired lock."""
    if lock is not None:
        if lock.stream is None or lock.path != layout.root / "writer.lock":
            raise ValueError("Recovery needs this run's active exclusive lock")
        return _recover(layout, expected, progress)
    with SnapshotLock(layout.root):
        return _recover(layout, expected, progress)
