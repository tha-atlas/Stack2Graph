"""Process bounded XML blocks in order with measured worker concurrency."""

from collections import deque
from collections.abc import Generator
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass
from multiprocessing import get_context
from time import perf_counter
from xml.parsers import expat

from sql.domain.models import TableBatch, XMLInput
from sql.domain.schema import TABLES
from sql.interfaces.store import BatchEncoder
from sql.parsing.envelope import EnvelopeRules
from sql.parsing.worker_selection import XMLWorkerProfile
from sql.parsing.xml_stream import XMLTableStream
from utils.logging_utils import get_logger
from utils.resources import MIB, BatchLimits, ResourceBudget, ResourcePlan, require_memory, worker_capacity
from utils.throughput import ThroughputTuner

LOGGER = get_logger(__name__)


@dataclass(frozen=True)
class XMLBlock:
    first_position: int
    row_count: int
    payload: bytes


@dataclass(frozen=True)
class ImportConcurrency:
    workers: int
    block_bytes: int
    worker_bytes: int


def import_concurrency(resources: ResourcePlan) -> ImportConcurrency:
    """Limit XML workers by CPU and RAM while reserving writer and COPY capacity."""
    block_bytes = min(32 * MIB, max(1, resources.batches.max_bytes // 4))
    worker_bytes = 128 * MIB + block_bytes * 16 + resources.batches.max_bytes * 4
    workers = worker_capacity(
        ResourceBudget(resources.threads, resources.memory_bytes, resources.evidence),
        worker_bytes,
        threads_per_worker=2,
        reserved_threads=1,
        reserved_memory_bytes=resources.database_bytes + resources.batches.max_bytes * 4,
    )
    if workers < 2:
        workers = 0
    return ImportConcurrency(workers, block_bytes, worker_bytes)


class XMLBlockStream:
    """
    Frame complete XML rows using dump lines or Expat byte positions.

    The reader validates the document envelope and EOF. Workers validate every
    row fragment. General XML layouts use Expat framing of the original
    document.
    """

    def __init__(self, source: XMLInput, limits: BatchLimits, target_bytes: int) -> None:
        """Store the XML source and block limits without opening the input."""
        self.source, self.limits, self.target_bytes = source, limits, target_bytes
        self.xml_rows: int | None = None

    def __iter__(self) -> Generator[XMLBlock]:
        """Yield XML blocks, falling back to Expat for unusual layouts."""
        emitted = yield from self._line_blocks()
        if self.xml_rows is None:
            yield from self._expat_blocks(emitted)

    def _line_blocks(self) -> Generator[XMLBlock, None, int]:
        """
        Frame usual dump lines without decoding their attributes in the reader.

        Workers validate every fragment with Expat. If the layout changes, reframe
        the document with Expat, omitting already emitted rows.
        """
        root = TABLES[self.source.table].xml_root.encode("ascii")
        emitted, position, rows, first = 0, 0, 0, 1
        buffer = bytearray()
        token_limit = self.limits.max_record_bytes + 64 * 1024
        with self.source.path.open("rb") as stream:
            header = stream.readline(token_limit + 1)
            if header.strip().startswith(b"<?xml") and header.strip().endswith(b"?>"):
                header += stream.readline(token_limit + 1)
            if not header or header.splitlines()[-1].strip() != b"<" + root + b">":
                return emitted
            parser = expat.ParserCreate("UTF-8")
            rules = EnvelopeRules(self.source.table, TABLES[self.source.table].xml_root, self.limits.max_record_bytes)
            rules.guard(parser)
            try:
                parser.Parse(header, False)
                while line := stream.readline(token_limit + 1):
                    if len(line) > token_limit:
                        return emitted
                    stripped = line.strip()
                    if (
                        stripped.startswith((b"<row ", b"<row\t"))
                        and stripped.endswith(b"/>")
                        and stripped.count(b"<row") == 1
                    ):
                        if rows and (len(buffer) + len(line) > self.target_bytes or rows >= self.limits.max_rows):
                            yield XMLBlock(first, rows, bytes(buffer))
                            emitted += rows
                            rows, buffer, first = 0, bytearray(), position + 1
                        buffer.extend(line)
                        rows += 1
                        position += 1
                    elif not stripped:
                        if len(buffer) + len(line) > self.target_bytes + token_limit:
                            return emitted
                        buffer.extend(line)
                    elif stripped == b"</" + root + b">":
                        parser.Parse(line, False)
                        while tail := stream.read(64 * 1024):
                            parser.Parse(tail, False)
                        parser.Parse(b"", True)
                        if rows:
                            yield XMLBlock(first, rows, bytes(buffer))
                            emitted += rows
                        self.xml_rows = position
                        return emitted
                    else:
                        return emitted
            except expat.ExpatError as error:
                raise ValueError(f"{self.source.table} position {position}: malformed XML: {error}") from error
        return emitted

    def _expat_blocks(self, skip_rows: int) -> Generator[XMLBlock]:
        """Frame validated XML byte ranges while omitting already emitted rows."""
        parser = expat.ParserCreate("UTF-8")
        rules = EnvelopeRules(self.source.table, TABLES[self.source.table].xml_root, self.limits.max_record_bytes)
        depth, position, rows, first, start_byte = 0, 0, 0, 0, 0
        root_seen, root_closed = False, False
        last_event = 0
        boundaries: deque[tuple[int, int, int, int]] = deque()

        def start(name: str, attributes: dict[str, str]) -> None:
            """Validate element starts and queue row-aligned block boundaries."""
            nonlocal depth, position, rows, first, start_byte, root_seen, last_event
            index = parser.CurrentByteIndex
            last_event = index
            if depth == 0:
                rules.check_root(root_seen, name, attributes)
                root_seen = True
            elif depth == 1 and name == "row":
                position += 1
                if position > skip_rows:
                    if rows and (index - start_byte >= self.target_bytes or rows >= self.limits.max_rows):
                        boundaries.append((start_byte, index, first, rows))
                        rows = 0
                    if not rows:
                        first, start_byte = position, index
                    rows += 1
            else:
                rules.nested(position, name)
            depth += 1

        def end(name: str) -> None:
            """Track element depth and queue the final block at root closure."""
            nonlocal depth, rows, root_closed, last_event
            last_event = parser.CurrentByteIndex
            depth -= 1
            if depth == 0:
                root_closed = True
                if rows:
                    boundaries.append((start_byte, last_event, first, rows))
                    rows = 0

        def text(value: str) -> None:
            """Check inter-row text and update the last parser event offset."""
            nonlocal last_event
            last_event = parser.CurrentByteIndex
            rules.text(position, value)

        parser.StartElementHandler, parser.EndElementHandler, parser.CharacterDataHandler = start, end, text
        rules.guard(parser)
        buffer = bytearray()
        base, consumed = 0, 0
        try:
            with self.source.path.open("rb") as stream:
                while True:
                    chunk = stream.read(min(64 * 1024, self.limits.max_record_bytes))
                    buffer.extend(chunk)
                    consumed += len(chunk)
                    parser.Parse(chunk, not chunk)
                    rules.check_token_budget(consumed, last_event, depth, position, root_closed)
                    for left, right, first_position, row_count in boundaries:
                        yield XMLBlock(first_position, row_count, bytes(buffer[left - base : right - base]))
                    boundaries.clear()
                    keep = start_byte if rows else min(consumed, parser.CurrentByteIndex)
                    if keep > base:
                        del buffer[: keep - base]
                        base = keep
                    if len(buffer) > self.target_bytes + self.limits.max_record_bytes + 128 * 1024:
                        raise ValueError(f"{self.source.table}: XML block exceeds bounded record headroom")
                    if not chunk:
                        break
            if not root_seen or not root_closed or depth:
                raise ValueError(f"{self.source.table}: incomplete XML")
            self.xml_rows = position
        except expat.ExpatError as error:
            raise rules.malformed(position, error) from error


def decode_block(source: XMLInput, limits: BatchLimits, block: XMLBlock, skip_rows: int) -> tuple[TableBatch, ...]:
    """Decode an XML block and validate resumed rows at their original positions."""
    end_position = block.first_position + block.row_count - 1
    stream = XMLTableStream(source, limits, min(skip_rows, end_position))
    root = TABLES[source.table].xml_root.encode("ascii")
    batches = tuple(
        stream.parse_chunks((b"<" + root + b">", block.payload, b"</" + root + b">"), block.first_position - 1)
    )
    if stream.xml_rows != end_position:
        raise ValueError(f"{source.table}: fragment row count mismatch")
    return batches


def identity_batch(batch: TableBatch, limits: BatchLimits) -> TableBatch:
    """Retain domain batches for callers that do not need adapter encoding."""
    return batch


def prepare_block[Batch](
    source: XMLInput, limits: BatchLimits, block: XMLBlock, skip_rows: int, encoder: BatchEncoder[Batch]
) -> tuple[Batch, ...]:
    """Decode an XML block and encode each resulting batch for the adapter."""
    return tuple(encoder(batch, limits) for batch in decode_block(source, limits, block, skip_rows))


class ParallelXMLTableStream[Batch]:
    def __init__(
        self,
        source: XMLInput,
        resources: ResourcePlan,
        skip_rows: int = 0,
        *,
        encoder: BatchEncoder[Batch],
        workers: int | None = None,
        profile: XMLWorkerProfile | None = None,
    ) -> None:
        """Set resume position, worker budget, and per-table concurrency tuning."""
        if skip_rows < 0 or source.table not in TABLES:
            raise ValueError("Unsupported table or negative resume position")
        self.source, self.resources, self.skip_rows = source, resources, skip_rows
        self.encoder = encoder
        self.profile = profile if workers is None else None
        self.xml_rows: int | None = None
        if workers is not None and workers <= 0:
            raise ValueError("XML workers must be positive")
        selected = workers if workers is not None else profile.load() if profile is not None else None
        maximum = max(1, import_concurrency(resources).workers)
        initial = max(1, min(2, maximum))
        if selected is not None:
            initial = min(selected, maximum)
            LOGGER.info(
                "%s XML workers: %s from %s (requested %s; ceiling %s)",
                source.table,
                initial,
                "configuration" if workers is not None else "saved profile",
                selected,
                maximum,
            )
        self.tuner = ThroughputTuner(
            maximum, unit="bytes", width=initial, best_width=initial, settled=selected is not None or maximum == 1
        )

    def _save_selection(self) -> None:
        """Persist the calibrated worker count when a profile is configured."""
        if self.profile is not None:
            self.profile.save(self.tuner.width)
            LOGGER.info("%s XML workers: saved %s; calibration complete", self.source.table, self.tuner.width)

    def __iter__(self) -> Generator[Batch]:
        """Yield ordered encoded batches and close all workers on exit."""
        plan = import_concurrency(self.resources)
        if self.tuner.width == 1 or (not self.tuner.settled and self.source.size < plan.block_bytes * 2):
            self.tuner.width = self.tuner.best_width = 1
            self.tuner.settled = True
            serial = XMLTableStream(self.source, self.resources.batches, self.skip_rows)
            for batch in serial:
                yield self.encoder(batch, self.resources.batches)
            self.xml_rows = serial.xml_rows
            return
        blocks = XMLBlockStream(self.source, self.resources.batches, plan.block_bytes)
        pending: deque[tuple[Future[tuple[Batch, ...]], int]] = deque()
        iterator = iter(blocks)
        exhausted = False
        LOGGER.info(
            "Parallel XML: up to %s workers, %s-byte blocks; %s concurrency",
            self.tuner.width if self.tuner.settled else plan.workers,
            plan.block_bytes,
            "fixed" if self.tuner.settled else "one-time calibration",
        )
        executor = ProcessPoolExecutor(
            max_workers=self.tuner.width if self.tuner.settled else plan.workers, mp_context=get_context("spawn")
        )
        try:
            while pending or not exhausted:
                started = perf_counter()
                while not exhausted and len(pending) < self.tuner.width:
                    try:
                        block = next(iterator)
                    except StopIteration:
                        exhausted = True
                        break
                    require_memory(plan.worker_bytes + self.resources.batches.max_bytes * 2)
                    pending.append(
                        (
                            executor.submit(
                                prepare_block, self.source, self.resources.batches, block, self.skip_rows, self.encoder
                            ),
                            len(block.payload),
                        )
                    )
                if pending:
                    future, payload_bytes = pending.popleft()
                    batches = future.result()
                    yield from batches
                    if batches:
                        settled = self.tuner.settled
                        self.tuner.observe(payload_bytes, perf_counter() - started)
                        if not settled and self.tuner.settled:
                            self._save_selection()
            self.xml_rows = blocks.xml_rows
            if self.xml_rows is None or self.xml_rows < self.skip_rows:
                raise ValueError(f"{self.source.table}: resume exceeds XML row count")
            if not self.tuner.settled and self.tuner.best_rate > 0:
                self.tuner.width, self.tuner.settled = self.tuner.best_width, True
                self._save_selection()
        finally:
            iterator.close()
            for future, _ in pending:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)
