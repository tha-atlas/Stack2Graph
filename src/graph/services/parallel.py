"""Run bounded jobs with process-local readers and compact worker results."""

from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, replace
from multiprocessing import get_context
from multiprocessing.shared_memory import SharedMemory
from types import TracebackType

from graph.adapter.postgres_source import PostgreSQLGraphSource
from graph.domain.artifacts import ArtifactLayout, RangeMarker
from graph.domain.config import ExportConfig, RunIdentity
from graph.domain.source import QuestionRange
from graph.mapping.content import ContentMapper
from graph.mapping.identity import NAMESPACE, IriPolicy
from graph.resources import GraphResourcePlan
from graph.schema.contract import SchemaContract
from graph.schema.ontology import new_schema_graph
from selection.languages import LanguageSelector
from selection.registry import load_selection_registry
from utils.logging_utils import RunLog, current_run, get_logger, stage_event, worker_logging
from utils.resources import ResourcePlan

_COUNTER_BYTES = 8


def _counter_view(memory: SharedMemory) -> memoryview[int]:
    """Expose shared progress memory as unsigned 64-bit counters."""
    buffer = memory.buf
    if buffer is None:
        raise RuntimeError("Question progress shared memory is closed")
    return buffer.cast("Q")


@dataclass(frozen=True)
class QuestionProgressSlot:
    name: str
    index: int

    @contextmanager
    def track(self) -> Iterator[Callable[[int], None]]:
        """Provide a progress callback and release its shared-memory view on exit."""
        memory = SharedMemory(name=self.name)
        counters = _counter_view(memory)

        def update(completed: int) -> None:
            """Replace this slot's completed-question count."""
            counters[self.index] = completed

        try:
            yield update
        finally:
            counters.release()
            memory.close()


class SharedQuestionProgress:
    """Expose per-job progress counters without locks to the controller process."""

    def __init__(self, slots: int) -> None:
        """Allocate and zero the requested number of shared progress slots."""
        if slots < 1:
            raise ValueError("Question progress requires at least one slot")
        self.slots = slots
        self.memory = SharedMemory(create=True, size=slots * _COUNTER_BYTES)
        self.counters = _counter_view(self.memory)
        for index in range(slots):
            self.counters[index] = 0

    def __enter__(self) -> SharedQuestionProgress:
        """Provide the shared progress owner for the context."""
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release counter views, close shared memory, and unlink its storage."""
        del exception_type, exception, traceback
        self.counters.release()
        self.memory.close()
        try:
            self.memory.unlink()
        except FileNotFoundError:
            pass

    def slot(self, index: int) -> QuestionProgressSlot:
        """Return a process-safe reference to a valid progress slot."""
        if index < 0 or index >= self.slots:
            raise IndexError("Question progress slot is out of range")
        return QuestionProgressSlot(self.memory.name, index)

    def reset(self, index: int) -> None:
        """Reset one slot's completed-question count to zero."""
        self.counters[index] = 0

    def completed(self, slots: Iterable[int]) -> int:
        """Sum completed-question counts for the supplied slot indices."""
        return sum(self.counters[index] for index in slots)


@dataclass(frozen=True)
class RangeJob:
    config: ExportConfig
    identity: RunIdentity
    part: QuestionRange
    resources: ResourcePlan

    def __call__(self, progress: QuestionProgressSlot | None = None) -> RangeMarker:
        """Verify worker provenance, export one range, and close its source."""
        from graph.services.export import build_identity, export_range

        iris = IriPolicy()
        source = PostgreSQLGraphSource(
            self.config.paths,
            self.resources,
            replace(self.config.postgres, pool_size=1),
            LanguageSelector(load_selection_registry(self.config.registry_path)),
            iris,
            snapshot_id=self.identity.snapshot_id,
        )
        try:
            self.identity.require_match(build_identity(self.config, source), context=f"Worker range {self.part.key}")
            content = ContentMapper(SchemaContract.from_ontology(new_schema_graph(NAMESPACE)), iris)
            if progress is None:
                return export_range(
                    source,
                    content,
                    ArtifactLayout(self.config.run_root),
                    self.part,
                    self.resources,
                    self.config.shard_bytes,
                    self.config.compression,
                    self.config.compression_level,
                )
            with progress.track() as update:
                return export_range(
                    source,
                    content,
                    ArtifactLayout(self.config.run_root),
                    self.part,
                    self.resources,
                    self.config.shard_bytes,
                    self.config.compression,
                    self.config.compression_level,
                    update,
                )
        finally:
            source.close()


def execute_job(job: RangeJob, run: RunLog | None = None, progress: QuestionProgressSlot | None = None) -> RangeMarker:
    """Run one range job with worker logging and completion or failure events."""
    with worker_logging(run):
        logger = get_logger(__name__)
        try:
            marker = job(progress)
        except BaseException:
            logger.exception("Range %s failed", job.part.key)
            stage_event("range_failed", range_key=job.part.key)
            raise
        stage_event("range_completed", range_key=job.part.key, questions=marker.question_count)
        return marker


class ParallelExporter:
    def run(
        self,
        jobs: Iterable[RangeJob],
        plan: GraphResourcePlan,
        *,
        progress: Callable[[int], None] | None = None,
        initial: int = 0,
    ) -> Iterator[RangeMarker]:
        """Yield range results from bounded workers with optional shared progress."""
        if progress is None:
            yield from self._run(jobs, plan, None, None, initial)
            return
        with SharedQuestionProgress(plan.pending_jobs) as shared:
            yield from self._run(jobs, plan, shared, progress, initial)

    def _run(
        self,
        jobs: Iterable[RangeJob],
        plan: GraphResourcePlan,
        shared: SharedQuestionProgress | None,
        progress: Callable[[int], None] | None,
        initial: int,
    ) -> Iterator[RangeMarker]:
        """Schedule bounded range jobs and terminate workers if processing fails."""
        iterator = iter(jobs)
        with ProcessPoolExecutor(max_workers=plan.workers, mp_context=get_context("spawn")) as executor:
            pending: dict[Future[RangeMarker], int | None] = {}
            available = list(range(plan.pending_jobs))
            finished_questions = 0
            exhausted = False
            try:
                while pending or not exhausted:
                    while not exhausted and len(pending) < plan.pending_jobs:
                        job = next(iterator, None)
                        if job is None:
                            exhausted = True
                        else:
                            if shared is not None:
                                slot = available.pop()
                                shared.reset(slot)
                                progress_slot = shared.slot(slot)
                            else:
                                slot = None
                                progress_slot = None
                            future = executor.submit(
                                execute_job,
                                job,
                                current_run(),
                                progress_slot,
                            )
                            pending[future] = slot
                    if not pending:
                        break
                    completed, _ = wait(
                        pending,
                        timeout=0.2 if progress is not None else None,
                        return_when=FIRST_COMPLETED,
                    )
                    if progress is not None and shared is not None:
                        active_slots = tuple(slot for slot in pending.values() if slot is not None)
                        progress(initial + finished_questions + shared.completed(active_slots))
                    for future in completed:
                        slot = pending.pop(future)
                        marker = future.result()
                        finished_questions += marker.question_count
                        if slot is not None:
                            assert shared is not None
                            shared.reset(slot)
                            available.append(slot)
                        yield marker
                if progress is not None:
                    progress(initial + finished_questions)
            except BaseException:
                for future in pending:
                    future.cancel()
                executor.terminate_workers()
                raise
