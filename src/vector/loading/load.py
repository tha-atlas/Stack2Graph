"""Upload embedded shards and mark each loaded after all batches complete."""

import time
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from functools import partial
from threading import Event
from typing import Protocol

import numpy as np

from utils.artifacts import atomic_write_json
from vector.domain.loading import LoadSettings
from vector.domain.models import CollectionSchema, CollectionState, VectorPoint
from vector.interfaces.loading import PointStore
from vector.loading.points import point_id
from vector.retry import retry_operation
from vector.shards import Manifest, RunLayout, StageMarker, read_dense, read_rows, read_sparse, try_claim
from vector.workers import process_claimed


class _Rows(Protocol):
    def tolist(self) -> list[list[float]]:
        """Convert matrix rows to nested lists of floats."""
        ...


def ensure_collection(
    layout: RunLayout, store: PointStore, dense_dimension: int, confirm: Callable[[CollectionState], bool]
) -> None:
    """Recreate the target collection once per run, after confirmation if it exists."""
    if layout.collection_created.exists():
        return
    state = store.inspect()
    if state is not None and not confirm(state):
        raise RuntimeError("Existing collection rebuild was not confirmed")
    store.replace(state, CollectionSchema(dense_dimension, build_id=layout.root.name))
    atomic_write_json(layout.collection_created, {"dense_dimension": dense_dimension})


def shard_batches(
    layout: RunLayout, shard: int, manifest: Manifest, batch_points: int
) -> Iterator[tuple[VectorPoint, ...]]:
    """Yield point batches from a shard's chunk, dense, and sparse files."""
    rows = read_rows(layout.chunk_rows(shard))
    dense = read_dense(layout.dense_rows(shard), len(rows), manifest.dense_dimension)
    sparse = read_sparse(layout, shard, len(rows))
    digest = manifest.point_digest
    for start in range(0, len(rows), batch_points):
        end = min(start + batch_points, len(rows))
        vectors: _Rows = dense[start:end].astype(np.float32)
        yield tuple(
            VectorPoint(
                point_id(
                    manifest.snapshot_digest, digest, row["payload"]["question_id"], row["payload"]["chunk_index"]
                ),
                tuple(vector),
                sparse.row(start + offset),
                row["payload"],
            )
            for offset, (row, vector) in enumerate(zip(rows[start:end], vectors.tolist(), strict=True))
        )


def load_shard(
    layout: RunLayout, shard: int, manifest: Manifest, store: PointStore, settings: LoadSettings
) -> StageMarker:
    """Upload a shard with bounded concurrent retries and return its marker."""
    started = time.perf_counter()
    count = 0
    with ThreadPoolExecutor(max_workers=settings.upload_workers, thread_name_prefix="qdrant-upload") as pool:
        pending: set[Future[None]] = set()
        try:
            for batch in shard_batches(layout, shard, manifest, settings.upload_points):
                while len(pending) >= settings.upload_workers * 2:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        future.result()
                pending.add(pool.submit(retry_operation, partial(store.upload, batch), settings.attempts))
                count += len(batch)
            for future in pending:
                future.result()
        except BaseException:
            pool.shutdown(wait=True, cancel_futures=True)
            raise
    return StageMarker(count, time.perf_counter() - started)


def ready_shards(layout: RunLayout) -> tuple[int, ...]:
    """Return embedded shard IDs that have not been marked loaded."""
    return tuple(
        shard
        for shard in layout.finished_chunk_shards()
        if layout.dense_done(shard).exists()
        and layout.sparse_done(shard).exists()
        and not layout.loaded_done(shard).exists()
    )


def claim_ready_shard(layout: RunLayout) -> int | None:
    """Claim a ready load shard, or return None if none is available."""
    for shard in ready_shards(layout):
        if try_claim(layout.claim("load", shard)):
            return shard
    return None


def run_load(
    layout: RunLayout,
    manifest: Manifest,
    store: PointStore,
    settings: LoadSettings,
    *,
    follow: bool,
    stop: Event | None = None,
    poll_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    parent_alive: Callable[[], bool] | None = None,
) -> int:
    """
    Claim and load embedded shards, optionally following extraction.

    With `follow`, wait until extraction is complete and every shard is loaded
    or held by another loader.
    """
    loaded = 0
    while stop is None or not stop.is_set():
        if parent_alive is not None and not parent_alive():
            raise RuntimeError("The loader stopped because its parent process exited")
        complete = layout.extract_complete.exists()
        shard = claim_ready_shard(layout)
        if shard is None:
            settled = all(
                layout.loaded_done(other).exists() or layout.claim("load", other).exists()
                for other in layout.finished_chunk_shards()
            )
            if not follow or (complete and settled):
                return loaded
            sleep(poll_seconds)
            continue
        loaded += process_claimed(
            layout, "load", shard, lambda pending: load_shard(layout, pending, manifest, store, settings)
        ).rows
    raise RuntimeError("Loading stopped because another stage failed")
