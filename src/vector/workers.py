"""Supervise GPU and loader processes and record their failures."""

from collections.abc import Callable, Sequence
from multiprocessing.process import BaseProcess
from os import getppid
from threading import Event

from utils.artifacts import atomic_write_bytes, atomic_write_json
from vector.shards import ClaimStage, RunLayout, StageMarker


def process_claimed(
    layout: RunLayout, stage: ClaimStage, shard: int, handle: Callable[[int], StageMarker]
) -> StageMarker:
    """Run a claimed shard, mark success, release its claim, and return its marker."""
    try:
        marker = handle(shard)
        atomic_write_json(layout.stage_done(stage, shard), marker.to_json())
        return marker
    finally:
        layout.claim(stage, shard).unlink(missing_ok=True)


def parent_process_alive() -> Callable[[], bool]:
    """Build a check that the recorded spawning process remains the parent."""
    parent = getppid()
    return lambda: getppid() == parent


def guarded_worker(layout: RunLayout, name: str, body: Callable[[], None]) -> None:
    """Run a worker body and record failures for the supervisor's run log."""
    try:
        body()
    except BaseException as error:
        atomic_write_bytes(layout.worker_error(name), f"{type(error).__name__}: {error}".encode())
        raise


def worker_failure(layout: RunLayout, name: str) -> str | None:
    """Read a worker's failure report, or return None if absent."""
    path = layout.worker_error(name)
    return path.read_text(encoding="utf-8") if path.exists() else None


def supervise_processes(
    layout: RunLayout, workers: Sequence[tuple[str, BaseProcess]], *, stop: Event | None = None
) -> None:
    """Start and await workers; terminate all on the first failure or stop."""
    for _name, process in workers:
        process.start()
    try:
        while True:
            failed = [(name, process) for name, process in workers if process.exitcode not in (None, 0)]
            if failed:
                name, process = failed[0]
                reason = worker_failure(layout, name) or "no error report"
                raise RuntimeError(f"Worker {name} exited with code {process.exitcode}: {reason}")
            running = [process for _name, process in workers if process.exitcode is None]
            if not running:
                return
            if stop is not None and stop.is_set():
                raise RuntimeError("Workers stopped because another stage failed")
            running[0].join(timeout=1.0)
    finally:
        for _name, process in workers:
            if process.exitcode is None:
                process.terminate()
        for _name, process in workers:
            process.join(timeout=30)
            if process.exitcode is None:
                process.kill()
                process.join(timeout=5)
