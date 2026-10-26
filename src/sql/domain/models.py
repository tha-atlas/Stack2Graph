"""Define snapshot identities and streamed records without database drivers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias, TypedDict

from utils.types import JSONObject, canonical_hash, json_array, json_integer, json_string

RowValue: TypeAlias = str | int | bool | dict[str, str] | None
Row: TypeAlias = dict[str, RowValue]


class BinaryEvidence(TypedDict):
    path: str
    version: str


@dataclass(frozen=True)
class ArchiveChecksum:
    """Record the downloaded source archive's checksum."""

    size: int
    sha256: str

    def to_json(self) -> JSONObject:
        """Serialize the archive size and SHA256 checksum."""
        return {"size": self.size, "sha256": self.sha256}

    @classmethod
    def from_dict(cls, value: JSONObject) -> ArchiveChecksum:
        """Decode the archive size and SHA256 checksum."""
        return cls(json_integer(value["size"]), json_string(value["sha256"]))


@dataclass(frozen=True)
class SnapshotIdentity:
    item_id: str
    release_date: str
    archive_name: str
    size: int
    checksums: tuple[tuple[str, str], ...]
    publisher: str
    uploader: str
    evidence: tuple[tuple[str, str], ...]

    @property
    def digest(self) -> str:
        """The canonical hash of this snapshot's provenance fields."""
        return canonical_hash(self.to_json(), ensure_ascii=False)

    def to_json(self) -> JSONObject:
        """Serialize snapshot provenance, checksums, and publisher evidence."""
        return {
            "item_id": self.item_id,
            "release_date": self.release_date,
            "archive_name": self.archive_name,
            "size": self.size,
            "checksums": [[key, value] for key, value in self.checksums],
            "publisher": self.publisher,
            "uploader": self.uploader,
            "evidence": [[key, value] for key, value in self.evidence],
        }

    @classmethod
    def from_dict(cls, value: JSONObject) -> SnapshotIdentity:
        """Decode snapshot provenance and validated string-pair collections."""

        def pairs(name: str) -> tuple[tuple[str, str], ...]:
            """Decode an array of two-string key/value pairs."""
            result: list[tuple[str, str]] = []
            for entry in json_array(value[name]):
                pair = json_array(entry)
                if len(pair) != 2:
                    raise ValueError(f"Expected a key/value pair in {name}")
                result.append((json_string(pair[0]), json_string(pair[1])))
            return tuple(result)

        return cls(
            json_string(value["item_id"]),
            json_string(value["release_date"]),
            json_string(value["archive_name"]),
            json_integer(value["size"]),
            pairs("checksums"),
            json_string(value["publisher"]),
            json_string(value["uploader"]),
            pairs("evidence"),
        )


@dataclass(frozen=True)
class XMLInput:
    table: str
    path: Path
    size: int


@dataclass(frozen=True)
class XMLInventory:
    inputs: tuple[XMLInput, ...]
    archive_checksum: ArchiveChecksum

    @property
    def digest(self) -> str:
        """The canonical hash of selected XML files and the archive checksum."""
        return canonical_hash(
            {
                "inputs": sorted((x.table, x.path.name, x.size) for x in self.inputs),
                "archive": self.archive_checksum.to_json(),
            },
            ensure_ascii=False,
        )


@dataclass(frozen=True)
class TableBatch:
    table: str
    first_position: int
    rows: tuple[Row, ...]


@dataclass(frozen=True)
class EncodedTableBatch:
    """Hold adapter-encoded COPY text without exposing third-party types."""

    table: str
    first_position: int
    row_count: int
    payload: bytes


@dataclass(frozen=True)
class ImportProgress:
    table: str
    position: int = 0
    count: int = 0
    eof: bool = False

    def to_json(self) -> JSONObject:
        """Serialize table position, committed count, and EOF state."""
        return {"table": self.table, "position": self.position, "count": self.count, "eof": self.eof}


@dataclass(frozen=True)
class ValidationReport:
    identity: str
    inventory: str
    counts: tuple[tuple[str, int], ...]
    runtime: tuple[str, ...]
    # XML parser counters from the durable import ledger, carried beside the
    # database counts so the public gate compares both sides of one scan.
    xml_counts: tuple[tuple[str, int], ...] = ()

    def to_json(self) -> JSONObject:
        """Serialize snapshot identity, table counts, and runtime evidence."""
        return {
            "identity": self.identity,
            "inventory": self.inventory,
            "counts": [[table, count] for table, count in self.counts],
            "xml_counts": [[table, count] for table, count in self.xml_counts],
            "runtime": list(self.runtime),
        }

    @classmethod
    def from_dict(cls, value: JSONObject) -> ValidationReport:
        """Decode validation identities, count pairs, and runtime versions."""

        def pairs(key: str) -> tuple[tuple[str, int], ...]:
            """Decode optional table/count pairs, returning an empty tuple if absent."""
            rows: list[tuple[str, int]] = []
            raw = value.get(key)
            if raw is None:
                return ()
            for entry in json_array(raw):
                pair = json_array(entry)
                if len(pair) != 2:
                    raise ValueError("Expected a table/count pair")
                rows.append((json_string(pair[0]), json_integer(pair[1])))
            return tuple(rows)

        return cls(
            json_string(value["identity"]),
            json_string(value["inventory"]),
            pairs("counts"),
            tuple(json_string(entry) for entry in json_array(value["runtime"])),
            pairs("xml_counts"),
        )


@dataclass(frozen=True)
class PublishedSnapshot:
    schema: str
    identity: SnapshotIdentity
    validation: ValidationReport
