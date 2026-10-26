"""Write selected PostgreSQL question chunks to immutable JSONL shards."""

import os
from collections.abc import Iterable, Iterator
from concurrent.futures import Executor
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Literal

from selection.languages import LanguageSelector
from selection.registry import load_selection_registry
from utils.artifacts import atomic_write_json
from utils.logging_utils import get_logger
from vector.domain.config import ChunkConfig
from vector.domain.models import PreparedChunk, QuestionRecord
from vector.interfaces.source import QuestionBatches
from vector.interfaces.token_budget import TokenBudget
from vector.loading.points import make_payload
from vector.preparation.chunking import ChunkingError, prepare_question
from vector.preparation.languages import select_question
from vector.shards import ChunkMarker, ChunkRow, RowsWriter, RunLayout, read_json_file

LOGGER = get_logger(__name__)
FIRST_ID = -(1 << 63)
Status = Literal["selected", "not_selected", "chunking_error"]


@dataclass(frozen=True)
class WorkerState:
    selector: LanguageSelector
    budgets: tuple[TokenBudget, TokenBudget]
    chunks: ChunkConfig
    snapshot_digest: str


_STATE: dict[str, WorkerState] = {}


def install_worker_state(state: WorkerState) -> None:
    """Set the current process's extraction state."""
    _STATE["current"] = state


def initialize_worker(
    registry_path: Path, dense_path: Path, sparse_path: Path, chunks: ChunkConfig, snapshot_digest: str
) -> None:
    """Process-pool initializer: load tokenizers once per worker process."""
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    os.environ["RAYON_NUM_THREADS"] = "1"
    from vector.preparation.token_budgets import load_token_budgets

    install_worker_state(
        WorkerState(
            LanguageSelector(load_selection_registry(registry_path)),
            load_token_budgets(dense_path, sparse_path),
            chunks,
            snapshot_digest,
        )
    )


@dataclass(frozen=True)
class QuestionOutcome:
    question_id: int
    rows: tuple[ChunkRow, ...]
    status: Status


def chunk_row(prepared: PreparedChunk, snapshot_digest: str) -> ChunkRow:
    """Combine prepared text, token counts, and snapshot payload metadata."""
    return {
        "embedding_text": prepared.chunk.embedding_text,
        "dense_tokens": prepared.dense_tokens,
        "sparse_tokens": prepared.sparse_tokens,
        "payload": make_payload(prepared.chunk, snapshot_digest),
    }


def process_question(question: QuestionRecord, state: WorkerState) -> QuestionOutcome:
    """Select and chunk a question, recording skipped or failed chunking."""
    selected = select_question(question, state.selector)
    if selected is None:
        return QuestionOutcome(question.question_id, (), "not_selected")
    try:
        prepared = prepare_question(selected, state.chunks, state.budgets)
    except ChunkingError as error:
        LOGGER.warning("Skipping question %s: %s", question.question_id, error)
        return QuestionOutcome(question.question_id, (), "chunking_error")
    rows = tuple(chunk_row(item, state.snapshot_digest) for item in prepared)
    return QuestionOutcome(question.question_id, rows, "selected")


def process_batch(batch: tuple[QuestionRecord, ...]) -> tuple[QuestionOutcome, ...]:
    """Process a question batch using the initialized worker state."""
    state = _STATE.get("current")
    if state is None:
        raise RuntimeError("Extract worker state was not initialized")
    return tuple(process_question(item, state) for item in batch)


@dataclass(frozen=True)
class ExtractResult:
    shards: int
    rows: int
    scanned: int


@dataclass
class _OpenShard:
    writer: RowsWriter
    first_question_id: int
    last_question_id: int = 0
    scanned: int = 0
    not_selected: int = 0
    chunking_errors: int = 0

    def add(self, outcome: QuestionOutcome) -> None:
        """Write outcome rows and update question and error counters."""
        for row in outcome.rows:
            self.writer.write(row)
        self.last_question_id = outcome.question_id
        self.scanned += 1
        if outcome.status == "not_selected":
            self.not_selected += 1
        elif outcome.status == "chunking_error":
            self.chunking_errors += 1

    def publish(self, layout: RunLayout, shard: int) -> ChunkMarker:
        """Publish chunk rows and their completion marker."""
        self.writer.publish()
        marker = ChunkMarker(
            self.first_question_id,
            self.last_question_id,
            self.writer.rows,
            self.scanned,
            self.not_selected,
            self.chunking_errors,
        )
        atomic_write_json(layout.chunk_done(shard), marker.to_json())
        return marker


def write_shards(
    layout: RunLayout,
    outcomes: Iterable[tuple[QuestionOutcome, ...]],
    shard_children: int,
    first_shard: int,
    *,
    stop: Event | None = None,
) -> ExtractResult:
    """Close consecutive shards at question boundaries after enough children."""
    shard, current = first_shard, None
    rows = scanned = 0
    try:
        for batch in outcomes:
            if stop is not None and stop.is_set():
                raise RuntimeError("Extraction stopped because another stage failed")
            for outcome in batch:
                if current is None:
                    current = _OpenShard(RowsWriter(layout.chunk_rows(shard)), outcome.question_id)
                current.add(outcome)
                if current.writer.rows >= shard_children:
                    marker = current.publish(layout, shard)
                    rows, scanned, shard, current = rows + marker.rows, scanned + marker.scanned, shard + 1, None
        if current is not None:
            marker = current.publish(layout, shard)
            rows, scanned, shard, current = rows + marker.rows, scanned + marker.scanned, shard + 1, None
    except BaseException:
        if current is not None:
            current.writer.discard()
        raise
    return ExtractResult(shard - first_shard, rows, scanned)


def _markers(layout: RunLayout) -> list[ChunkMarker]:
    """Read chunk markers and reject noncontiguous shard IDs."""
    shards = layout.finished_chunk_shards()
    if shards != tuple(range(len(shards))):
        raise RuntimeError(f"Chunk shards in {layout.chunks} are not contiguous: {shards}")
    return [ChunkMarker.from_json(read_json_file(layout.chunk_done(shard))) for shard in shards]


def _limited(
    batches: Iterator[tuple[QuestionRecord, ...]], remaining: int | None
) -> Iterator[tuple[QuestionRecord, ...]]:
    """Yield batches up to the optional total question limit."""
    if remaining is None:
        yield from batches
        return
    for batch in batches:
        if remaining <= 0:
            return
        yield batch[:remaining]
        remaining -= len(batch)


def run_extract(
    layout: RunLayout,
    source: QuestionBatches,
    executor: Executor,
    *,
    shard_children: int,
    read_rows: int,
    after_id: int = FIRST_ID,
    question_limit: int | None = None,
    buffered_batches: int = 64,
    stop: Event | None = None,
) -> ExtractResult:
    """
    Continue after the last finished shard and mark extraction complete at the end.

    Args:
        layout: Run paths for shards and extraction completion markers.
        source: Pinned batches of source questions.
        executor: Executor with initialized question-processing workers.
        shard_children: Target child count before closing at a question boundary.
        read_rows: Maximum requested question batch size.
        after_id: Exclusive starting ID for a new extraction.
        question_limit: Optional total number of source questions to scan.
        buffered_batches: Maximum number of submitted batches held pending.
        stop: Optional event requesting an early stop.

    Returns:
        Extraction counters after continuing from completed shard markers.

    """
    layout.create()
    markers = _markers(layout)
    if not layout.extract_complete.exists():
        start_after = markers[-1].last_question_id if markers else after_id
        scanned = sum(marker.scanned for marker in markers)
        remaining = None if question_limit is None else question_limit - scanned
        batches = _limited(source.iter_vector_questions(read_rows, after_id=start_after), remaining)
        outcomes = executor.map(process_batch, batches, buffersize=buffered_batches)
        write_shards(layout, outcomes, shard_children, len(markers), stop=stop)
        markers = _markers(layout)
        result = ExtractResult(len(markers), sum(m.rows for m in markers), sum(m.scanned for m in markers))
        atomic_write_json(
            layout.extract_complete, {"shards": result.shards, "rows": result.rows, "scanned": result.scanned}
        )
        return result
    LOGGER.info("Extraction already complete for run %s", layout.root.name)
    return ExtractResult(len(markers), sum(m.rows for m in markers), sum(m.scanned for m in markers))
