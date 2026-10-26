"""Define explicit build settings and fixed identities that determine output."""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from sql.domain.postgres import PostgreSQLConfig
from utils.config import DataPaths
from utils.resources import ResourceCaps
from utils.types import JSONObject, canonical_hash, json_integer, json_string


@dataclass(frozen=True)
class ExportConfig:
    postgres: PostgreSQLConfig
    paths: DataPaths
    registry_path: Path
    run_root: Path
    range_width: int
    limits: ResourceCaps
    shard_bytes: int
    snapshot_id: str
    compression: Literal["none", "gzip"] = "none"
    compression_level: int | None = None

    def __post_init__(self) -> None:
        """Validate compression, snapshot identity, and positive export limits."""
        if (self.compression == "none" and self.compression_level is not None) or (
            self.compression == "gzip" and (self.compression_level is None or not 1 <= self.compression_level <= 9)
        ):
            raise ValueError("Compression needs none/no level or gzip/a level from 1 to 9")
        if self.range_width < 1 or self.shard_bytes < 1 or not self.snapshot_id:
            raise ValueError("Export needs a fixed snapshot and positive range/shard budgets")
        if not self.run_root.is_absolute():
            raise ValueError("Graph run root must be absolute")


@dataclass(frozen=True)
class RunIdentity:
    snapshot_id: str
    snapshot_digest: str
    registry_digest: str
    schema_source_digest: str
    ontology_digest: str
    mapping_version: str
    exporter_digest: str
    range_width: int
    compression: Literal["none", "gzip"]
    compression_level: int | None
    shard_bytes: int = 128 * 1024**2

    def to_json(self) -> JSONObject:
        """Serialize source, schema, exporter, and artifact-format identity fields."""
        return {
            "snapshot_id": self.snapshot_id,
            "snapshot_digest": self.snapshot_digest,
            "registry_digest": self.registry_digest,
            "schema_source_digest": self.schema_source_digest,
            "ontology_digest": self.ontology_digest,
            "mapping_version": self.mapping_version,
            "exporter_digest": self.exporter_digest,
            "range_width": self.range_width,
            "compression": self.compression,
            "compression_level": self.compression_level,
            "shard_bytes": self.shard_bytes,
        }

    @property
    def digest(self) -> str:
        """The canonical hash of the complete run identity."""
        return canonical_hash(self.to_json())

    def require_match(self, observed: RunIdentity, *, context: str) -> None:
        """Reject changed run settings and identify source or exporter changes."""
        current = observed.to_json()
        changed = tuple(key for key, expected in self.to_json().items() if current[key] != expected)
        if changed:
            raise ValueError(
                f"{context}: export identity changed ({', '.join(changed)}). "
                "Keep source data, registry, exporter code and dependency versions unchanged throughout the build."
            )

    @classmethod
    def from_json(cls, value: JSONObject) -> RunIdentity:
        """Decode run identity and validate its compression kind."""
        compression = json_string(value["compression"])
        if compression not in ("none", "gzip"):
            raise ValueError("Unsupported build compression")
        level = value["compression_level"]
        kind: Literal["none", "gzip"] = "none" if compression == "none" else "gzip"
        return cls(
            json_string(value["snapshot_id"]),
            json_string(value["snapshot_digest"]),
            json_string(value["registry_digest"]),
            json_string(value["schema_source_digest"]),
            json_string(value["ontology_digest"]),
            json_string(value["mapping_version"]),
            json_string(value["exporter_digest"]),
            json_integer(value["range_width"]),
            kind,
            None if level is None else json_integer(level),
            json_integer(value["shard_bytes"]),
        )
