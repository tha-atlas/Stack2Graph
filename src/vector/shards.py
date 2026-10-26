"""Define run paths, fixed manifests, shard files, markers, and worker claims."""

import json
import math
import os
import re
import shutil
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import accumulate, pairwise
from pathlib import Path
from typing import BinaryIO, Literal, Protocol, TypedDict

import numpy as np
from numpy.typing import NDArray

from utils.artifacts import AtomicBinaryWriter, atomic_write_json
from utils.manifest import FileLock
from utils.types import (
    JSONObject,
    JSONValue,
    canonical_hash,
    json_array,
    json_integer,
    json_object,
    json_string,
    parse_json,
)
from vector.domain.models import PointPayload, SparseWeights

ClaimStage = Literal["dense", "sparse", "load"]
UINT32_MAX = 2**32 - 1
_SHARD = re.compile(r"shard-(\d{5})")


def shard_name(shard: int) -> str:
    """Format a shard ID from 0 through 99999 as a padded filename stem."""
    if not 0 <= shard <= 99999:
        raise ValueError("Shard index must be between 0 and 99999")
    return f"shard-{shard:05d}"


@dataclass(frozen=True)
class RunLayout:
    """Locate a run's files and markers, which follow completed data writes."""

    root: Path

    @property
    def manifest(self) -> Path:
        """The run manifest path."""
        return self.root / "manifest.json"

    @property
    def lock(self) -> Path:
        """The exclusive run lock path."""
        return self.root / "run.lock"

    @property
    def chunks(self) -> Path:
        """The chunk shard directory."""
        return self.root / "chunks"

    @property
    def embeddings(self) -> Path:
        """The dense and sparse embedding directory."""
        return self.root / "embeddings"

    @property
    def loaded(self) -> Path:
        """The directory containing loaded-shard markers."""
        return self.root / "loaded"

    @property
    def extract_complete(self) -> Path:
        """The extraction completion marker path."""
        return self.chunks / "complete.done"

    @property
    def collection_created(self) -> Path:
        """The collection creation marker path."""
        return self.root / "collection.created"

    @property
    def finalized(self) -> Path:
        """The run finalization marker path."""
        return self.root / "finalized.done"

    @property
    def report(self) -> Path:
        """The run report path."""
        return self.root / "report.json"

    def create(self) -> None:
        """Create the chunk, embedding, and loaded-marker directories."""
        for directory in (self.chunks, self.embeddings, self.loaded):
            directory.mkdir(parents=True, exist_ok=True)

    def chunk_rows(self, shard: int) -> Path:
        """Return the JSONL chunk-row path for a shard."""
        return self.chunks / f"{shard_name(shard)}.jsonl"

    def chunk_done(self, shard: int) -> Path:
        """Return the chunk completion marker path for a shard."""
        return self.chunks / f"{shard_name(shard)}.done"

    def dense_rows(self, shard: int) -> Path:
        """Return the float16 dense-vector path for a shard."""
        return self.embeddings / f"{shard_name(shard)}.dense.f16"

    def dense_done(self, shard: int) -> Path:
        """Return the dense embedding completion marker path for a shard."""
        return self.embeddings / f"{shard_name(shard)}.dense.done"

    def sparse_offsets(self, shard: int) -> Path:
        """Return the int64 sparse offset path for a shard."""
        return self.embeddings / f"{shard_name(shard)}.sparse.offsets.i64"

    def sparse_indices(self, shard: int) -> Path:
        """Return the uint32 sparse index path for a shard."""
        return self.embeddings / f"{shard_name(shard)}.sparse.indices.u32"

    def sparse_values(self, shard: int) -> Path:
        """Return the float32 sparse weight path for a shard."""
        return self.embeddings / f"{shard_name(shard)}.sparse.values.f32"

    def sparse_done(self, shard: int) -> Path:
        """Return the sparse embedding completion marker path for a shard."""
        return self.embeddings / f"{shard_name(shard)}.sparse.done"

    def stage_done(self, stage: ClaimStage, shard: int) -> Path:
        """Return the completion marker path for the requested shard stage."""
        if stage == "dense":
            return self.dense_done(shard)
        return self.sparse_done(shard) if stage == "sparse" else self.loaded_done(shard)

    def loaded_done(self, shard: int) -> Path:
        """Return the loaded-shard completion marker path."""
        return self.loaded / f"{shard_name(shard)}.done"

    def claim(self, stage: ClaimStage, shard: int) -> Path:
        """Return the claim-file path for a shard and stage."""
        directory = self.loaded if stage == "load" else self.embeddings
        return directory / f"{shard_name(shard)}.claimed-{stage}"

    def worker_ready(self, name: str) -> Path:
        """Return the model-load timing file path for a worker such as gpu3."""
        return self.embeddings / f"worker-{name}.json"

    def worker_error(self, name: str) -> Path:
        """Return the failure report path for a worker such as gpu3 or load1."""
        return self.embeddings / f"worker-{name}.error"

    def finished_chunk_shards(self) -> tuple[int, ...]:
        """Return sorted shard IDs that have chunk completion markers."""
        if not self.chunks.is_dir():
            return ()
        found: list[int] = []
        for path in self.chunks.glob("shard-*.done"):
            match = _SHARD.fullmatch(path.name.removesuffix(".done"))
            if match is not None:
                found.append(int(match.group(1)))
        return tuple(sorted(found))


def read_json_file(path: Path) -> JSONObject:
    """Read a UTF-8 JSON object from a file."""
    return json_object(parse_json(path.read_text(encoding="utf-8")))


def remove_temporary(layout: RunLayout) -> int:
    """Delete partial writes left by a crash; complete files are kept."""
    removed = 0
    for directory in (layout.root, layout.chunks, layout.embeddings, layout.loaded):
        if directory.is_dir():
            for path in directory.glob("*.tmp"):
                path.unlink()
                removed += 1
    return removed


def reset_run(layout: RunLayout, *, overwrite: bool) -> None:
    """Remove run artifacts after overwrite consent, preserving its held lock."""
    if layout.root.is_symlink():
        raise ValueError("Run directory must not be a symlink")
    if not layout.root.is_dir():
        return
    existing = any(path != layout.lock for path in layout.root.iterdir())
    if existing and not overwrite:
        raise RuntimeError(f"Run {layout.root.name} already has output; pass --overwrite to rebuild it")
    for path in layout.root.iterdir():
        if path == layout.lock:
            continue
        if path.is_symlink() or not path.is_dir():
            path.unlink()
        else:
            shutil.rmtree(path)


@dataclass(frozen=True)
class Manifest:
    """Keep output settings fixed from the moment a run is created."""

    snapshot_id: str
    snapshot_digest: str
    registry_digest: str
    dense_model: str
    dense_revision: str
    sparse_model: str
    sparse_revision: str
    dense_dimension: int
    chunk_tokens: int
    chunk_overlap: int
    collection: str
    after_id: int
    question_limit: int | None

    def to_json(self) -> JSONObject:
        """Serialize snapshot, model, selection, and source-window settings."""
        return {
            "snapshot_id": self.snapshot_id,
            "snapshot_digest": self.snapshot_digest,
            "registry_digest": self.registry_digest,
            "dense_model": self.dense_model,
            "dense_revision": self.dense_revision,
            "sparse_model": self.sparse_model,
            "sparse_revision": self.sparse_revision,
            "dense_dimension": self.dense_dimension,
            "chunk_tokens": self.chunk_tokens,
            "chunk_overlap": self.chunk_overlap,
            "collection": self.collection,
            "after_id": self.after_id,
            "question_limit": self.question_limit,
        }

    @classmethod
    def from_json(cls, value: JSONObject) -> Manifest:
        """Decode typed manifest fields from a JSON object."""
        limit = value["question_limit"]
        return cls(
            json_string(value["snapshot_id"]),
            json_string(value["snapshot_digest"]),
            json_string(value["registry_digest"]),
            json_string(value["dense_model"]),
            json_string(value["dense_revision"]),
            json_string(value["sparse_model"]),
            json_string(value["sparse_revision"]),
            json_integer(value["dense_dimension"]),
            json_integer(value["chunk_tokens"]),
            json_integer(value["chunk_overlap"]),
            json_string(value["collection"]),
            json_integer(value["after_id"]),
            None if limit is None else json_integer(limit),
        )

    @property
    def point_digest(self) -> str:
        """The point-ID settings digest, independent of the target collection."""
        identity = self.to_json()
        del identity["collection"]
        return canonical_hash(identity)


class ManifestMismatchError(ValueError):
    """Report that a run was created with different output settings."""


_LATER_STAGE_UNKNOWN = ("snapshot_digest", "after_id", "question_limit")


def check_manifest(layout: RunLayout, expected: Manifest, *, create: bool) -> Manifest:
    """
    Create or compare the manifest and return the stored one.

    Without `create` (stages after extract), the snapshot digest and question
    range are not known from configuration and are not compared.

    Args:
        layout: Run layout containing the manifest.
        expected: Identity and configuration to create or compare.
        create: Allow initial creation and compare source digest and ID window.

    Returns:
        The stored manifest. With create=False, source digest and ID window
        are retained from disk because later stages do not resolve them.

    Raises:
        ValueError: Recorded run settings conflict with the expected settings.

    """
    if not layout.manifest.exists():
        if not create:
            raise RuntimeError(f"Run {layout.root.name} has no manifest; run the extract stage first")
        layout.root.mkdir(parents=True, exist_ok=True)
        atomic_write_json(layout.manifest, expected.to_json())
        return expected
    stored = Manifest.from_json(read_json_file(layout.manifest))
    previous, current = stored.to_json(), expected.to_json()
    ignored = () if create else _LATER_STAGE_UNKNOWN
    differing = sorted(key for key in current if key not in ignored and current[key] != previous[key])
    if differing:
        raise ManifestMismatchError(
            f"Run {layout.root.name} was created with different settings: {', '.join(differing)}"
        )
    return stored


class ChunkRow(TypedDict):
    embedding_text: str
    dense_tokens: int
    sparse_tokens: int
    payload: PointPayload


def encode_row(row: ChunkRow) -> str:
    """Serialize a chunk row as compact Unicode JSON."""
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"))


def _optional_integer(value: JSONValue | None) -> int | None:
    """Decode a JSON integer, preserving None."""
    return None if value is None else json_integer(value)


def _optional_string(value: JSONValue | None) -> str | None:
    """Decode a JSON string, preserving None."""
    return None if value is None else json_string(value)


def _boolean(value: JSONValue) -> bool:
    """Require a JSON boolean or raise ValueError."""
    if not isinstance(value, bool):
        raise ValueError("Expected a JSON boolean")
    return value


def json_number(value: JSONValue) -> float:
    """Convert a JSON number to float, rejecting booleans and other types."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError("Expected a JSON number")
    return float(value)


def decode_row(line: str) -> ChunkRow:
    """Decode chunk text, token counts, and typed payload fields from JSON."""
    data = json_object(parse_json(line))
    payload = json_object(data["payload"])
    return {
        "embedding_text": json_string(data["embedding_text"]),
        "dense_tokens": json_integer(data["dense_tokens"]),
        "sparse_tokens": json_integer(data["sparse_tokens"]),
        "payload": {
            "question_id": json_integer(payload["question_id"]),
            "snapshot_id": json_string(payload["snapshot_id"]),
            "chunk_index": json_integer(payload["chunk_index"]),
            "chunk_count": json_integer(payload["chunk_count"]),
            "tags": [json_string(tag) for tag in json_array(payload["tags"])],
            "languages": [json_string(language) for language in json_array(payload["languages"])],
            "score": _optional_integer(payload.get("score")),
            "answer_count": _optional_integer(payload.get("answer_count")),
            "accepted_answer_id": _optional_integer(payload.get("accepted_answer_id")),
            "has_accepted_answer": _boolean(payload["has_accepted_answer"]),
            "is_closed": _boolean(payload["is_closed"]),
            "creation_date": _optional_string(payload.get("creation_date")),
            "last_edit_date": _optional_string(payload.get("last_edit_date")),
            "last_activity_date": _optional_string(payload.get("last_activity_date")),
            "closed_date": _optional_string(payload.get("closed_date")),
        },
    }


def read_rows(path: Path) -> tuple[ChunkRow, ...]:
    """Read nonblank JSONL records into a tuple of chunk rows."""
    with path.open(encoding="utf-8") as handle:
        return tuple(decode_row(line) for line in handle if line.strip())


class RowsWriter:
    """Append rows to a temporary file and publish the complete file atomically."""

    def __init__(self, path: Path) -> None:
        """Open an atomic shard writer and initialize its row count."""
        self.path = path
        self._writer = AtomicBinaryWriter(path)
        self.rows = 0

    def write(self, row: ChunkRow) -> None:
        """Append one JSONL chunk row and increment the row count."""
        self._writer.stream.write((encode_row(row) + "\n").encode("utf-8"))
        self.rows += 1

    def publish(self) -> None:
        """Atomically publish the completed row file."""
        self._writer.publish()

    def discard(self) -> None:
        """Close the writer and remove its temporary file."""
        self._writer.discard()


@dataclass(frozen=True)
class ChunkMarker:
    first_question_id: int
    last_question_id: int
    rows: int
    scanned: int
    not_selected: int
    chunking_errors: int

    def to_json(self) -> JSONObject:
        """Serialize question bounds and chunk-processing counters."""
        return {
            "first_question_id": self.first_question_id,
            "last_question_id": self.last_question_id,
            "rows": self.rows,
            "scanned": self.scanned,
            "not_selected": self.not_selected,
            "chunking_errors": self.chunking_errors,
        }

    @classmethod
    def from_json(cls, value: JSONObject) -> ChunkMarker:
        """Decode question bounds and chunk-processing counters."""
        return cls(
            json_integer(value["first_question_id"]),
            json_integer(value["last_question_id"]),
            json_integer(value["rows"]),
            json_integer(value["scanned"]),
            json_integer(value["not_selected"]),
            json_integer(value["chunking_errors"]),
        )


@dataclass(frozen=True)
class StageMarker:
    rows: int
    seconds: float
    device: int | None = None

    def to_json(self) -> JSONObject:
        """Serialize row count, duration, and optional device ID."""
        return {"rows": self.rows, "seconds": self.seconds, "device": self.device}

    @classmethod
    def from_json(cls, value: JSONObject) -> StageMarker:
        """Decode stage row count, duration, and optional device ID."""
        device = value.get("device")
        return cls(
            json_integer(value["rows"]), json_number(value["seconds"]), None if device is None else json_integer(device)
        )


class _FileArray(Protocol):
    def tofile(self, fid: BinaryIO, /) -> None:
        """Write array values to an open binary stream."""
        ...


def _write_array(path: Path, array: _FileArray) -> None:
    """Atomically write array bytes and discard temporary files on exit."""
    writer = AtomicBinaryWriter(path)
    try:
        array.tofile(writer.stream)
        writer.publish()
    finally:
        writer.discard()


def write_dense(path: Path, matrix: NDArray[np.float16]) -> None:
    """Write a two-dimensional dense matrix as contiguous float16 values."""
    if matrix.ndim != 2:
        raise ValueError("Dense output must be a two-dimensional matrix")
    _write_array(path, np.ascontiguousarray(matrix))


def read_dense(path: Path, rows: int, dimension: int) -> NDArray[np.float16]:
    """Read float16 values and validate the expected matrix shape."""
    values = np.fromfile(path, dtype=np.float16)
    if values.size != rows * dimension:
        raise ValueError(f"{path.name} holds {values.size} values; expected {rows} x {dimension}")
    return values.reshape(rows, dimension)


class _Integers(Protocol):
    def tolist(self) -> list[int]:
        """Convert array values to a list of integers."""
        ...


class _Floats(Protocol):
    def tolist(self) -> list[float]:
        """Convert array values to a list of floats."""
        ...


@dataclass(frozen=True)
class SparseShard:
    """
    Store sparse rows as shared index and value arrays with row offsets.

    Row i uses the span offsets[i]:offsets[i + 1] in both arrays.
    """

    offsets: tuple[int, ...]
    indices: NDArray[np.uint32]
    values: NDArray[np.float32]

    @property
    def rows(self) -> int:
        """The number of sparse rows described by the offsets."""
        return len(self.offsets) - 1

    def row(self, index: int) -> SparseWeights:
        """Return the sparse indices and weights at a row index."""
        start, end = self.offsets[index], self.offsets[index + 1]
        indices: _Integers = self.indices[start:end]
        values: _Floats = self.values[start:end]
        return SparseWeights(tuple(indices.tolist()), tuple(values.tolist()))


def sparse_shard(rows: Sequence[SparseWeights]) -> SparseShard:
    """Validate sparse rows and pack them into indexed arrays."""
    for row in rows:
        if (
            len(row.indices) != len(row.values)
            or list(row.indices) != sorted(set(row.indices))
            or any(not 0 <= index <= UINT32_MAX for index in row.indices)
            or any(not math.isfinite(value) or value <= 0 for value in row.values)
        ):
            raise ValueError("Sparse rows need ascending unique uint32 indices and finite positive weights")
    offsets = (0, *accumulate(len(row.indices) for row in rows))
    total = offsets[-1]
    indices = np.fromiter((index for row in rows for index in row.indices), dtype=np.uint32, count=total)
    values = np.fromiter((value for row in rows for value in row.values), dtype=np.float32, count=total)
    return SparseShard(offsets, indices, values)


def write_sparse(layout: RunLayout, shard: int, data: SparseShard) -> None:
    """Write a shard's offsets, token indices, and sparse weights atomically."""
    _write_array(layout.sparse_offsets(shard), np.asarray(data.offsets, dtype=np.int64))
    _write_array(layout.sparse_indices(shard), data.indices)
    _write_array(layout.sparse_values(shard), data.values)


def read_sparse(layout: RunLayout, shard: int, rows: int) -> SparseShard:
    """Read sparse arrays and validate offsets against the expected row count."""
    stored: _Integers = np.fromfile(layout.sparse_offsets(shard), dtype=np.int64)
    offsets = tuple(stored.tolist())
    indices = np.fromfile(layout.sparse_indices(shard), dtype=np.uint32)
    values = np.fromfile(layout.sparse_values(shard), dtype=np.float32)
    if (
        len(offsets) != rows + 1
        or offsets[0] != 0
        or offsets[-1] != len(indices)
        or len(indices) != len(values)
        or any(end < start for start, end in pairwise(offsets))
    ):
        raise ValueError(f"Sparse files of {shard_name(shard)} are inconsistent with {rows} rows")
    return SparseShard(offsets, indices, values)


def try_claim(path: Path) -> bool:
    """Create an exclusive claim file, returning False if it already exists."""
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        return False
    os.close(descriptor)
    return True


def release_stale_claims(layout: RunLayout) -> int:
    """Remove crashed-worker claims and errors while holding the run lock."""
    released = 0
    stale = [
        path
        for directory in (layout.embeddings, layout.loaded)
        if directory.is_dir()
        for path in directory.glob("*.claimed-*")
    ]
    if layout.embeddings.is_dir():
        stale.extend(layout.embeddings.glob("worker-*.error"))
    for path in stale:
        path.unlink()
        released += 1
    return released


@contextmanager
def run_lock(layout: RunLayout) -> Generator[None]:
    """Hold the exclusive run lock for the duration of the context."""
    with FileLock(layout.lock, f"Run {layout.root} is already in use by another process"):
        yield
