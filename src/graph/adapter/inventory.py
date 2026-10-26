"""Read catalogs and merge large inventories within memory and disk budgets."""

import heapq
import json
from collections.abc import Generator, Iterable, Iterator
from contextlib import ExitStack
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TextIO

from graph.adapter.budgets import ArtifactBudget
from graph.domain.artifacts import ArtifactLayout, ArtifactManifest, FileRef, ShardCatalogRef, ShardRecord
from graph.domain.source import TagUsage
from utils.types import json_array, json_integer, json_object, json_string, parse_json


def file_ref(layout: ArtifactLayout, path: Path) -> FileRef:
    """Hash a file and return its relative path, SHA256, and byte count."""
    digest = sha256()
    size = 0
    with path.open("rb") as stream:
        while block := stream.read(1024**2):
            digest.update(block)
            size += len(block)
    return FileRef(path.relative_to(layout.root).as_posix(), digest.hexdigest(), size)


def verify_file(layout: ArtifactLayout, expected: FileRef) -> Path:
    """Resolve a file and reject mismatching size or checksum evidence."""
    path = layout.path(expected.path)
    if file_ref(layout, path) != expected:
        raise ValueError(f"Artifact checksum/size mismatch: {expected.path}")
    return path


def iter_shards(layout: ArtifactLayout, catalog: ShardCatalogRef) -> Iterator[ShardRecord]:
    """Yield catalog records after checking its checksum and final count."""
    path = layout.path(catalog.path)
    if file_ref(layout, path).sha256 != catalog.sha256:
        raise ValueError("Shard catalog checksum mismatch")
    count = 0
    with path.open() as stream:
        for line in stream:
            if len(line) > 1024**2:
                raise ValueError("Shard catalog record exceeds metadata budget")
            record = ShardRecord.from_json(json_object(parse_json(line)))
            layout.path(record.path)
            yield record
            count += 1
    if count != catalog.records:
        raise ValueError("Shard catalog count mismatch")


def _merged(paths: Iterable[Path]) -> Iterator[str]:
    """Merge sorted text files and yield each distinct line once."""
    with ExitStack() as stack:
        streams: list[TextIO] = [stack.enter_context(path.open()) for path in paths]
        previous: str | None = None
        for line in heapq.merge(*streams):
            if line != previous:
                yield line
                previous = line


def sorted_lines(
    lines: Iterable[str], directory: Path, memory_bytes: int, *, budget: ArtifactBudget | None = None
) -> Generator[str]:
    """
    Sort and deduplicate lines within memory and temporary-disk budgets.

    Args:
        lines: Text records to sort and deduplicate.
        directory: Parent directory for temporary sort files.
        memory_bytes: Maximum estimated bytes retained for each sort chunk.
        budget: Disk and spill allowance; None uses the default allowance.

    Yields:
        Unique lines in lexical order. Temporary files are removed on exit.

    """
    if memory_bytes < 1:
        raise ValueError("External sort memory must be positive")
    budget = ArtifactBudget.from_resources() if budget is None else budget
    directory.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="sort-", dir=directory) as name:
        root = Path(name)
        count, size = 0, 0
        chunk: list[str] = []

        def spill() -> None:
            """Write a sorted unique chunk and reset the in-memory buffer."""
            nonlocal count, size
            with budget.open_text(root / f"0-{count}") as output:
                output.writelines(sorted(set(chunk)))
            count += 1
            chunk.clear()
            size = 0

        try:
            for line in lines:
                # Account for Python string/list overhead as well as encoded bytes.
                needed = len(line.encode("utf-8")) * 4 + 128
                if chunk and size + needed > memory_bytes:
                    spill()
                chunk.append(line)
                size += needed
            if chunk:
                spill()
            level = 0
            while count > 32:
                next_count = 0
                for start in range(0, count, 32):
                    paths = tuple(root / f"{level}-{index}" for index in range(start, min(start + 32, count)))
                    with budget.open_text(root / f"{level + 1}-{next_count}") as output:
                        output.writelines(_merged(paths))
                    for path in paths:
                        budget.release_file(path)
                        path.unlink()
                    next_count += 1
                count = next_count
                level += 1
            yield from _merged(root / f"{level}-{index}" for index in range(count))
        finally:
            if root.exists():
                for path in root.iterdir():
                    budget.release_file(path)


def merge_tag_usage(
    paths: Iterable[Path], memory_bytes: int, *, budget: ArtifactBudget | None = None
) -> Iterator[TagUsage]:
    """Merge usage files into unique, sorted tag and graph memberships."""
    iterator = iter(paths)
    first = next(iterator, None)
    if first is None:
        return

    def lines() -> Iterator[str]:
        """Yield sortable usage lines from each supplied file."""

        def records(path: Path) -> Iterator[str]:
            """Decode tag usage records into sortable text lines."""
            with path.open() as stream:
                for line in stream:
                    values = json_array(parse_json(line))
                    tag = json_integer(values[0])
                    key = json_string(values[1])
                    yield f"{tag:020d}\t{json.dumps(key)}\n"

        yield from records(first)
        for path in iterator:
            yield from records(path)

    for line in sorted_lines(lines(), first.parent, memory_bytes, budget=budget):
        number, key = line.rstrip("\n").split("\t", 1)
        yield TagUsage(int(number), json_string(parse_json(key)))


class MembershipCatalog:
    """Find graph membership in fixed range metadata without source payloads."""

    def __init__(self, layout: ArtifactLayout, manifest: ArtifactManifest) -> None:
        """Store the artifact layout and manifest used for membership lookups."""
        self.layout, self.manifest = layout, manifest

    def membership(self, question_id: int) -> tuple[str, ...] | None:
        """Read a question's graph memberships, or return None if absent."""
        for marker in self.manifest.iter_ranges(self.layout):
            if marker.part.first_id <= question_id < marker.part.end_id:
                with verify_file(self.layout, marker.membership).open() as stream:
                    for line in stream:
                        value = json_object(parse_json(line))
                        current = json_integer(value["question_id"])
                        if current == question_id:
                            return tuple(json_string(key) for key in json_array(value["graph_keys"]))
                        if current > question_id:
                            return None
                return None
        return None
