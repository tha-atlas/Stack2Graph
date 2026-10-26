"""Build an empty native index without assigning it a source snapshot."""

from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from graph.adapter.inventory import file_ref, verify_file
from graph.adapter.qlever import QLeverAdapter
from graph.domain.artifacts import ArtifactLayout, FileRef
from utils.artifacts import atomic_write_json, fsync_directory
from utils.types import json_array, json_object, json_string, parse_json


class EmptyIndex:
    def __init__(self, runtime: QLeverAdapter, root: Path) -> None:
        """Derive the empty index path and owned runtime name."""
        self.runtime = runtime
        identity = sha256((str(root) + runtime.settings.image_digest).encode()).hexdigest()[:20]
        self.path = root / ("empty-" + identity)
        self.name = runtime.settings.server_name or "stack2graph-kg-empty-" + identity

    def ensure(self) -> Path:
        """Reuse a verified empty index or build and publish one atomically."""
        version = self.runtime.version()
        if self.path.exists():
            self._verify(version)
            return self.path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix=".empty-building-", dir=self.path.parent) as temporary:
            directory = Path(temporary)
            (directory / "inputs.jsonl").write_bytes(b"")
            atomic_write_json(
                directory / "kg.settings.json", {"num-triples-per-batch": 10000, "parser-batch-size": 1000}
            )
            settings = replace(
                self.runtime.settings,
                index_memory_bytes=min(self.runtime.settings.index_memory_bytes, 512 * 1024**2),
            )
            QLeverAdapter(settings).build_empty(directory)
            files = [file_ref(ArtifactLayout(directory), path).to_json() for path in sorted(directory.glob("kg.*"))]
            atomic_write_json(
                directory / "empty.json",
                {"image_digest": settings.image_digest, "cli_version": version, "files": files},
            )
            directory.rename(self.path)
            fsync_directory(self.path.parent)
        self._verify(version)
        return self.path

    def _verify(self, version: str) -> None:
        """Verify empty-index runtime identity and native file checksums."""
        value = json_object(parse_json((self.path / "empty.json").read_text()))
        if json_string(value["image_digest"]) != self.runtime.settings.image_digest or value["cli_version"] != version:
            raise ValueError("Empty QLever index runtime identity changed")
        if not json_array(value["files"]):
            raise ValueError("Empty QLever index has no native files")
        for item in json_array(value["files"]):
            verify_file(ArtifactLayout(self.path), FileRef.from_json(json_object(item)))
