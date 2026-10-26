"""Define portable artifact paths and persisted range and shard metadata."""

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from graph.domain.config import RunIdentity
from graph.domain.source import IMPORTED_TABLES, QuestionRange, SourceInventory
from sql.domain.models import SnapshotIdentity
from utils.types import JSONObject, json_array, json_integer, json_object, json_string, parse_json

RESERVED_GRAPH_KEYS: frozenset[str] = frozenset({"schema", "extra-languages"})
"""Named graphs that carry metadata rather than one language selection."""


def require_processed_table_counts(counts: tuple[tuple[str, int], ...]) -> tuple[tuple[str, int], ...]:
    """Require canonical source-row counts for all five tables."""
    if tuple(table for table, _ in counts) != IMPORTED_TABLES:
        raise ValueError("Processed counts must cover every imported table in canonical order")
    if any(count < 0 for _, count in counts):
        raise ValueError("Processed counts must not be negative")
    return counts


def processed_table_counts(counts: Mapping[str, int]) -> tuple[tuple[str, int], ...]:
    """Order partial per-table counters into the canonical imported-table tuple."""
    if set(counts) - set(IMPORTED_TABLES):
        raise ValueError("Processed counts reference tables that were not imported")
    if any(count < 0 for count in counts.values()):
        raise ValueError("Processed counts must not be negative")
    return tuple((table, counts.get(table, 0)) for table in IMPORTED_TABLES)


def combine_processed_table_counts(groups: Iterable[tuple[tuple[str, int], ...]]) -> tuple[tuple[str, int], ...]:
    """Sum groups that each count their source rows once."""
    totals = dict.fromkeys(IMPORTED_TABLES, 0)
    for group in groups:
        for table, count in require_processed_table_counts(group):
            totals[table] += count
    return tuple((table, totals[table]) for table in IMPORTED_TABLES)


def processed_table_counts_from_json(value: object) -> tuple[tuple[str, int], ...]:
    """Decode and validate processed source-table counts from JSON."""
    if not isinstance(value, list):
        raise ValueError("Expected a JSON array")
    entries: list[object] = value
    pairs: list[tuple[str, int]] = []
    for entry in entries:
        if not isinstance(entry, list):
            raise ValueError("Processed count entry must be a table/count pair")
        pair: list[object] = entry
        if len(pair) != 2:
            raise ValueError("Processed count entry must be a table/count pair")
        table, count = pair
        if not isinstance(table, str):
            raise ValueError("Processed count table must be a string")
        if not isinstance(count, int) or isinstance(count, bool):
            raise ValueError("Processed count must be an integer")
        pairs.append((table, count))
    return require_processed_table_counts(tuple(pairs))


@dataclass(frozen=True)
class ArtifactLayout:
    root: Path

    def path(self, relative: str) -> Path:
        """Resolve a relative artifact path, rejecting traversal and symlinks."""
        part = Path(relative)
        if part.is_absolute() or not relative or ".." in part.parts:
            raise ValueError(f"Unsafe artifact path: {relative}")
        path = self.root / part
        if path.resolve() != path.absolute() or not path.resolve().is_relative_to(self.root.resolve()):
            raise ValueError(f"Artifact path escapes root or follows symlink: {relative}")
        return path

    def range_marker(self, key: str) -> Path:
        """Return the completion marker path for a range key."""
        return self.path(f"ranges/{key}.json")

    @property
    def manifest(self) -> Path:
        """The run's artifact manifest path."""
        return self.path("manifest.json")


@dataclass(frozen=True)
class FileRef:
    path: str
    sha256: str
    bytes: int

    def to_json(self) -> JSONObject:
        """Serialize a relative file path, checksum, and byte count."""
        return {"path": self.path, "sha256": self.sha256, "bytes": self.bytes}

    @classmethod
    def from_json(cls, value: JSONObject) -> FileRef:
        """Decode a relative file path, checksum, and byte count."""
        return cls(json_string(value["path"]), json_string(value["sha256"]), json_integer(value["bytes"]))


@dataclass(frozen=True)
class ShardCatalogRef:
    path: str
    sha256: str
    records: int

    def to_json(self) -> JSONObject:
        """Serialize a shard catalog path, checksum, and record count."""
        return {"path": self.path, "sha256": self.sha256, "records": self.records}

    @classmethod
    def from_json(cls, value: JSONObject) -> ShardCatalogRef:
        """Decode a shard catalog path, checksum, and record count."""
        return cls(json_string(value["path"]), json_string(value["sha256"]), json_integer(value["records"]))


@dataclass(frozen=True)
class ShardRecord:
    path: str
    graph_key: str
    part_key: str
    bytes: int
    quads: int
    content_sha256: str
    stored_sha256: str
    compression: Literal["none", "gzip"]
    record_bytes: int

    def to_json(self) -> JSONObject:
        """Serialize shard ownership, counts, compression, and content checksums."""
        return {
            "path": self.path,
            "graph_key": self.graph_key,
            "part_key": self.part_key,
            "bytes": self.bytes,
            "quads": self.quads,
            "content_sha256": self.content_sha256,
            "stored_sha256": self.stored_sha256,
            "compression": self.compression,
            "record_bytes": self.record_bytes,
        }

    @classmethod
    def from_json(cls, value: JSONObject) -> ShardRecord:
        """Decode shard metadata and reject unsupported compression."""
        compression = json_string(value["compression"])
        if compression not in ("none", "gzip"):
            raise ValueError("Unsupported shard compression")
        return cls(
            json_string(value["path"]),
            json_string(value["graph_key"]),
            json_string(value["part_key"]),
            json_integer(value["bytes"]),
            json_integer(value["quads"]),
            json_string(value["content_sha256"]),
            json_string(value["stored_sha256"]),
            "none" if compression == "none" else "gzip",
            json_integer(value["record_bytes"]),
        )


@dataclass(frozen=True)
class RangeMarker:
    part: QuestionRange
    shard_catalog: ShardCatalogRef
    question_count: int
    usage: FileRef
    membership: FileRef
    processed_table_counts: tuple[tuple[str, int], ...]
    link_rows: int = 0
    distinct_links: int = 0

    def __post_init__(self) -> None:
        """Require canonical processed-row counts for all five source tables."""
        require_processed_table_counts(self.processed_table_counts)

    def to_json(self) -> JSONObject:
        """Serialize range bounds, artifact references, and processing counts."""
        return {
            "first_id": self.part.first_id,
            "end_id": self.part.end_id,
            "shards": self.shard_catalog.to_json(),
            "questions": self.question_count,
            "usage": self.usage.to_json(),
            "membership": self.membership.to_json(),
            "processed_table_counts": [[table, count] for table, count in self.processed_table_counts],
            "link_rows": self.link_rows,
            "distinct_links": self.distinct_links,
        }

    @classmethod
    def from_json(cls, value: JSONObject) -> RangeMarker:
        """Decode a range marker, requiring processed source-table counts."""
        if "processed_table_counts" not in value:
            raise ValueError("Range marker is missing processed_table_counts")
        return cls(
            QuestionRange(json_integer(value["first_id"]), json_integer(value["end_id"])),
            ShardCatalogRef.from_json(json_object(value["shards"])),
            json_integer(value["questions"]),
            FileRef.from_json(json_object(value["usage"])),
            FileRef.from_json(json_object(value["membership"])),
            processed_table_counts_from_json(value["processed_table_counts"]),
            json_integer(value["link_rows"]),
            json_integer(value["distinct_links"]),
        )


@dataclass(frozen=True)
class ArtifactManifest:
    identity: RunIdentity
    inventory: SourceInventory
    graph_keys: tuple[str, ...]
    range_catalog: FileRef
    shared_marker: FileRef
    schema_marker: FileRef
    complete: bool
    processed_table_counts: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        """Require canonical processed-row counts for all five source tables."""
        require_processed_table_counts(self.processed_table_counts)

    @property
    def language_keys(self) -> tuple[str, ...]:
        """Manifest graph keys without the reserved metadata graphs."""
        return tuple(key for key in self.graph_keys if key not in RESERVED_GRAPH_KEYS)

    def to_json(self) -> JSONObject:
        """Serialize provenance, source inventory, artifact references, and counts."""
        inventory = self.inventory
        return {
            "identity": self.identity.to_json(),
            "inventory": {
                "snapshot": inventory.snapshot.to_json(),
                "table_counts": [[table, count] for table, count in inventory.table_counts],
                "question_count": inventory.question_count,
                "first_question_id": inventory.first_question_id,
                "last_question_id": inventory.last_question_id,
                "exclusions": [[code, count] for code, count in inventory.exclusions],
                "orphan_documentation": inventory.orphan_documentation,
            },
            "processed_table_counts": [[table, count] for table, count in self.processed_table_counts],
            "graph_keys": list(self.graph_keys),
            "range_catalog": self.range_catalog.to_json(),
            "shared_marker": self.shared_marker.to_json(),
            "schema_marker": self.schema_marker.to_json(),
            "complete": self.complete,
        }

    @classmethod
    def from_json(cls, value: JSONObject) -> ArtifactManifest:
        """Decode manifest provenance and counts, requiring a boolean completion flag."""
        if "processed_table_counts" not in value:
            raise ValueError("Manifest is missing processed_table_counts")
        inventory = json_object(value["inventory"])

        def pairs(key: str) -> tuple[tuple[str, int], ...]:
            """Decode inventory entries as string/integer pairs."""
            result: list[tuple[str, int]] = []
            for entry in json_array(inventory[key]):
                pair = json_array(entry)
                result.append((json_string(pair[0]), json_integer(pair[1])))
            return tuple(result)

        def nullable(key: str) -> int | None:
            """Read an optional integer inventory field."""
            return None if inventory[key] is None else json_integer(inventory[key])

        complete = value["complete"]
        if not isinstance(complete, bool):
            raise ValueError("Manifest completion must be boolean")
        source = SourceInventory(
            SnapshotIdentity.from_dict(json_object(inventory["snapshot"])),
            pairs("table_counts"),
            json_integer(inventory["question_count"]),
            nullable("first_question_id"),
            nullable("last_question_id"),
            pairs("exclusions"),
            json_integer(inventory["orphan_documentation"]),
        )
        return cls(
            RunIdentity.from_json(json_object(value["identity"])),
            source,
            tuple(json_string(key) for key in json_array(value["graph_keys"])),
            FileRef.from_json(json_object(value["range_catalog"])),
            FileRef.from_json(json_object(value["shared_marker"])),
            FileRef.from_json(json_object(value["schema_marker"])),
            complete,
            processed_table_counts_from_json(value["processed_table_counts"]),
        )

    def iter_ranges(self, layout: ArtifactLayout) -> Iterator[RangeMarker]:
        """Verify the range catalog and yield bounded range-marker records."""
        from graph.adapter.inventory import verify_file

        with verify_file(layout, self.range_catalog).open() as stream:
            for line in stream:
                if len(line) > 1024**2:
                    raise ValueError("Range metadata exceeds budget")
                yield RangeMarker.from_json(json_object(parse_json(line)))
