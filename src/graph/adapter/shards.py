"""Write deterministic graph shards with bounded handles and atomic catalogs."""

import gzip
import json
from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass
from shutil import copyfileobj
from typing import BinaryIO, Literal
from urllib.parse import unquote

from pyoxigraph import NamedNode, Quad

from graph.adapter.budgets import ArtifactBudget
from graph.adapter.inventory import file_ref, sorted_lines
from graph.adapter.rdf_io import RdfIO
from graph.domain.artifacts import ArtifactLayout, ShardCatalogRef, ShardRecord
from graph.mapping.identity import BASE, IriPolicy
from utils.artifacts import AtomicBinaryWriter
from utils.resources import ResourceCaps, ResourcePlan


@dataclass
class _Shard:
    writer: AtomicBinaryWriter
    key: str
    part: int
    bytes: int = 0
    quads: int = 0
    record_bytes: int = 0


class GraphShardWriter:
    def __init__(
        self,
        layout: ArtifactLayout,
        part_key: str,
        *,
        target_bytes: int,
        max_open_files: int,
        compression: Literal["none", "gzip"] = "none",
        compression_level: int | None = None,
        resources: ResourcePlan | None = None,
    ) -> None:
        """Set shard, file-handle, compression, and artifact-budget limits."""
        if min(target_bytes, max_open_files) < 1 or not part_key or "/" in part_key or ".." in part_key:
            raise ValueError("Invalid shard writer configuration")
        self.compression, self.compression_level = compression, compression_level
        self.layout, self.part_key = layout, part_key
        self.target_bytes, self.max_open_files = target_bytes, max_open_files
        self._active: dict[str, _Shard] = {}
        self._open: OrderedDict[str, BinaryIO] = OrderedDict()
        self._next: dict[str, int] = {}
        self._catalog = AtomicBinaryWriter(layout.path(f"catalogs/{part_key}.jsonl"))
        self._records = 0
        self._finished = False
        self.budget = ArtifactBudget.from_resources(resources)
        source_limit = (
            resources.batches.max_record_bytes if resources else (ResourceCaps().record_bytes or 16 * 1024**2)
        )
        self._io = RdfIO(source_limit * 6 + 65536)
        self._iris = IriPolicy()

    @property
    def open_files(self) -> int:
        """The number of shard streams currently open."""
        return len(self._open)

    def _handle(self, key: str) -> _Shard:
        """Open or reuse a graph shard while respecting the file-handle limit."""
        if key not in self._active:
            part = self._next.get(key, 0)
            name = self._iris.graph(key)[len(BASE) :]
            writer = AtomicBinaryWriter(self.layout.path(f"graphs/{name}/{self.part_key}-{part:06d}.nq"))
            self._active[key] = _Shard(writer, key, part)
        shard = self._active[key]
        if key not in self._open:
            while len(self._open) >= self.max_open_files:
                _, stream = self._open.popitem(last=False)
                stream.flush()
                stream.close()
            if shard.writer.stream.closed:
                shard.writer.stream = shard.writer.temporary.open("ab")
            self._open[key] = shard.writer.stream
        self._open.move_to_end(key)
        return shard

    def _publish(self, key: str) -> None:
        """Publish one shard, optionally compress it, and append its catalog record."""
        shard = self._handle(key)
        shard.writer.publish()
        self._open.pop(key, None)
        reference = file_ref(self.layout, shard.writer.path)
        stored = reference
        if self.compression == "gzip":
            if self.compression_level is None:
                raise ValueError("Gzip compression requires an explicit level")
            compressed = AtomicBinaryWriter(shard.writer.path.with_suffix(".nq.gz"))
            try:
                self.budget.require(self.layout.root, reference.bytes + reference.bytes // 1000 + 65536)
                with (
                    shard.writer.path.open("rb") as source,
                    gzip.GzipFile(
                        filename="", mode="wb", fileobj=compressed.stream, compresslevel=self.compression_level, mtime=0
                    ) as output,
                ):
                    copyfileobj(source, output, 1024**2)
                compressed.publish()
                stored = file_ref(self.layout, compressed.path)
                shard.writer.path.unlink()
            finally:
                compressed.discard()
        record = ShardRecord(
            stored.path,
            key,
            self.part_key,
            stored.bytes,
            shard.quads,
            reference.sha256,
            stored.sha256,
            self.compression,
            shard.record_bytes,
        )
        data = (json.dumps(record.to_json(), sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        self.budget.require(self.layout.root, len(data))
        self._catalog.stream.write(data)
        self._records += 1
        self._next[key] = shard.part + 1
        del self._active[key]

    def write(self, quads: Iterable[Quad]) -> None:
        """Write canonical graph quads and rotate shards at the target byte size."""
        if self._finished:
            raise ValueError("Shard writer already finished")
        for quad in quads:
            if not isinstance(quad.graph_name, NamedNode) or not quad.graph_name.value.startswith(BASE):
                raise ValueError("Graph IRI is outside the approved namespace")
            key = unquote(quad.graph_name.value[len(BASE) :])
            if self._iris.graph(key) != quad.graph_name.value:
                raise ValueError("Noncanonical graph IRI")
            bound = self._io.record_bound(quad)
            self.budget.require(self.layout.root, bound)
            shard = self._handle(key)
            before = shard.writer.stream.tell()
            self._io.write((quad,), shard.writer.stream)
            shard.bytes += shard.writer.stream.tell() - before
            shard.quads += 1
            shard.record_bytes = max(shard.record_bytes, bound)
            if shard.bytes >= self.target_bytes:
                self._publish(key)

    def finish(self) -> ShardCatalogRef:
        """Publish remaining shards and a sorted catalog, returning its reference."""
        for key in sorted(self._active):
            self._publish(key)
        self._catalog.stream.flush()
        self._catalog.stream.close()
        ordered = AtomicBinaryWriter(self._catalog.path)
        try:
            with self._catalog.temporary.open() as stream:
                for line in sorted_lines(stream, self._catalog.path.parent, 1024**2, budget=self.budget):
                    ordered.stream.write(line.encode("utf-8"))
            ordered.publish()
        finally:
            ordered.discard()
            self._catalog.discard()
        self._finished = True
        reference = file_ref(self.layout, self._catalog.path)
        return ShardCatalogRef(reference.path, reference.sha256, self._records)

    def discard(self) -> None:
        """Close active writers and discard their temporary files and catalog."""
        for shard in self._active.values():
            shard.writer.discard()
        self._active.clear()
        self._open.clear()
        self._catalog.discard()
