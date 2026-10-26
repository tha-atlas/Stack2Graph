"""Select exported artifacts and record the fixed QLever runtime identity."""

import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from graph.domain.artifacts import FileRef
from graph.domain.config import RunIdentity
from utils.types import JSONObject, canonical_hash, json_array, json_object, json_string, parse_json


@dataclass(frozen=True)
class IndexSelection:
    language_keys: tuple[str, ...]


@dataclass(frozen=True)
class SelectedArtifacts:
    identity: RunIdentity
    shard_catalog: Path
    graph_keys: tuple[str, ...]
    input_digest: str
    source_root: Path

    def iter_paths(self) -> Iterator[Path]:
        """Yield selected input paths from the staged JSONL catalog."""
        with self.shard_catalog.open() as stream:
            for line in stream:
                yield Path(json_string(json_object(parse_json(line))["path"]))


@dataclass(frozen=True)
class QLeverSettings:
    executable: Path
    image_digest: str
    host: str
    port: int
    index_memory_bytes: int
    query_memory_bytes: int
    timeout_seconds: int
    index_timeout_seconds: int = 24 * 60 * 60
    instance_key: str = ""
    network: str = ""
    server_name: str = ""
    compose_project: str = "stack2graph-qlever"

    def __post_init__(self) -> None:
        """Validate immutable runtime identity, service names, and resource limits."""
        if re.fullmatch(r"[A-Za-z0-9./:_-]+@sha256:[0-9a-f]{64}", self.image_digest) is None:
            raise ValueError("QLever image must be an immutable full image reference with sha256 digest")
        if re.fullmatch(r"[A-Za-z0-9.-]+", self.host) is None or not 0 < self.port < 65536:
            raise ValueError("Invalid QLever host or port")
        if re.fullmatch(r"[a-z0-9-]{0,64}", self.instance_key) is None:
            raise ValueError("Invalid owned runtime instance key")
        if self.network and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", self.network) is None:
            raise ValueError("Invalid QLever gateway network")
        if self.server_name and re.fullmatch(r"stack2graph-[a-z0-9][a-z0-9-]{0,100}", self.server_name) is None:
            raise ValueError("Invalid QLever service container name")
        if re.fullmatch(r"[a-z0-9][a-z0-9_-]*", self.compose_project) is None:
            raise ValueError("Invalid QLever Compose project name")
        if min(self.index_memory_bytes, self.query_memory_bytes, self.timeout_seconds, self.index_timeout_seconds) < 1:
            raise ValueError("QLever budgets must be positive")


@dataclass(frozen=True)
class IndexConfig:
    artifact_root: Path
    index_root: Path
    settings: QLeverSettings


@dataclass(frozen=True)
class IndexManifest:
    index_id: str
    identity: RunIdentity
    graph_keys: tuple[str, ...]
    input_digest: str
    image_digest: str
    cli_version: str
    files: FileRef
    verified: bool

    def to_json(self) -> JSONObject:
        """Serialize index provenance, runtime identity, files, and verification state."""
        return {
            "index_id": self.index_id,
            "identity": self.identity.to_json(),
            "graph_keys": list(self.graph_keys),
            "input_digest": self.input_digest,
            "image_digest": self.image_digest,
            "cli_version": self.cli_version,
            "files": self.files.to_json(),
            "verified": self.verified,
        }

    @property
    def digest(self) -> str:
        """The canonical index hash excluding its mutable verification flag."""
        value = self.to_json()
        value.pop("verified")
        return canonical_hash(value)

    @classmethod
    def from_json(cls, value: JSONObject) -> IndexManifest:
        """Decode index metadata and require a boolean verification flag."""
        verified = value["verified"]
        if not isinstance(verified, bool):
            raise ValueError("Index verification must be boolean")
        return cls(
            json_string(value["index_id"]),
            RunIdentity.from_json(json_object(value["identity"])),
            tuple(json_string(key) for key in json_array(value["graph_keys"])),
            json_string(value["input_digest"]),
            json_string(value["image_digest"]),
            json_string(value["cli_version"]),
            FileRef.from_json(json_object(value["files"])),
            verified,
        )


@dataclass(frozen=True)
class QueryAcceptance:
    index_digest: str
    passed: bool
    checked_queries: tuple[str, ...]

    def to_json(self) -> JSONObject:
        """Serialize the tested index digest and query acceptance results."""
        return {"index_digest": self.index_digest, "passed": self.passed, "checked_queries": list(self.checked_queries)}
