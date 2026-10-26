"""Embed shards with sorted dense batches and whole-shard sparse encoding."""

import time
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event

import numpy as np
from numpy.typing import NDArray

from vector.domain.embedding import BatchTooLargeError, DenseBatching
from vector.embedding.batching import plan_batches
from vector.interfaces.embedding import DenseEncoder, SparseEncoder
from vector.shards import (
    ChunkRow,
    ClaimStage,
    RunLayout,
    StageMarker,
    read_rows,
    sparse_shard,
    try_claim,
    write_dense,
    write_sparse,
)
from vector.workers import process_claimed


def _texts(rows: Sequence[ChunkRow], indices: Sequence[int]) -> list[str]:
    """Return embedding texts at the requested row indices."""
    return [rows[index]["embedding_text"] for index in indices]


def _forward[Inputs](
    encoder: DenseEncoder[Inputs],
    rows: Sequence[ChunkRow],
    indices: tuple[int, ...],
    inputs: Inputs,
    output: NDArray[np.float16],
) -> None:
    """Fill output rows, splitting batches that exceed GPU memory."""
    try:
        vectors = encoder.forward(inputs)
    except BatchTooLargeError:
        vectors = None
    if vectors is None:
        # Split outside the except block so the failed batch's frames and tensors are released first.
        if len(indices) == 1:
            payload = rows[indices[0]]["payload"]
            raise RuntimeError(
                f"Question {payload['question_id']} chunk {payload['chunk_index']} does not fit on the GPU"
            )
        middle = len(indices) // 2
        for half in (indices[:middle], indices[middle:]):
            _forward(encoder, rows, half, encoder.tokenize(_texts(rows, half)), output)
        return
    if vectors.shape != (len(indices), output.shape[1]):
        raise ValueError(f"Dense output shape {vectors.shape} differs from ({len(indices)}, {output.shape[1]})")
    output[list(indices)] = vectors


def embed_dense_shard[Inputs](
    layout: RunLayout, shard: int, encoder: DenseEncoder[Inputs], batching: DenseBatching, device: int
) -> StageMarker:
    """Encode sorted batches while tokenizing the next batch alongside GPU work."""
    started = time.perf_counter()
    rows = read_rows(layout.chunk_rows(shard))
    output = np.zeros((len(rows), batching.dimension), dtype=np.float16)
    batches = plan_batches([row["dense_tokens"] for row in rows], batching.batch_tokens, batching.batch_rows)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="tokenize") as tokenizer:
        pending: Future[Inputs] | None = (
            tokenizer.submit(encoder.tokenize, _texts(rows, batches[0].rows)) if batches else None
        )
        for position, batch in enumerate(batches):
            if pending is None:
                raise RuntimeError("Tokenized batch is missing")
            inputs = pending.result()
            following = batches[position + 1] if position + 1 < len(batches) else None
            pending = tokenizer.submit(encoder.tokenize, _texts(rows, following.rows)) if following else None
            _forward(encoder, rows, batch.rows, inputs, output)
    write_dense(layout.dense_rows(shard), output)
    return StageMarker(len(rows), time.perf_counter() - started, device)


def embed_sparse_shard(layout: RunLayout, shard: int, encoder: SparseEncoder, device: int) -> StageMarker:
    """Encode a shard's sparse vectors and return its stage timing marker."""
    started = time.perf_counter()
    rows = read_rows(layout.chunk_rows(shard))
    weights = encoder.encode([row["embedding_text"] for row in rows]) if rows else ()
    if len(weights) != len(rows):
        raise ValueError("Sparse model output count differs from input count")
    write_sparse(layout, shard, sparse_shard(weights))
    return StageMarker(len(rows), time.perf_counter() - started, device)


def claim_next(layout: RunLayout, stage: ClaimStage) -> int | None:
    """Claim the next unfinished embedding stage, or return None."""
    for shard in layout.finished_chunk_shards():
        if not layout.stage_done(stage, shard).exists() and try_claim(layout.claim(stage, shard)):
            return shard
    return None


def gpu_worker_loop(
    layout: RunLayout,
    dense: Callable[[int], StageMarker],
    sparse: Callable[[int], StageMarker],
    *,
    follow: bool,
    stop: Event | None = None,
    poll_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    parent_alive: Callable[[], bool] | None = None,
) -> int:
    """
    Embed dense and sparse stages on one GPU using shard claims.

    Dense runs before sparse on each shard so it becomes loadable early.
    Sparse work left over from a restart is taken once no dense work remains.
    Returns processed stages.
    """
    processed = 0
    while stop is None or not stop.is_set():
        if parent_alive is not None and not parent_alive():
            raise RuntimeError("The GPU worker stopped because its parent process exited")
        complete = layout.extract_complete.exists()
        shard = claim_next(layout, "dense")
        if shard is not None:
            process_claimed(layout, "dense", shard, dense)
            processed += 1
            if not layout.sparse_done(shard).exists() and try_claim(layout.claim("sparse", shard)):
                process_claimed(layout, "sparse", shard, sparse)
                processed += 1
            continue
        shard = claim_next(layout, "sparse")
        if shard is not None:
            process_claimed(layout, "sparse", shard, sparse)
            processed += 1
            continue
        if complete or not follow:
            return processed
        sleep(poll_seconds)
    raise RuntimeError("The GPU worker stopped because another stage failed")
