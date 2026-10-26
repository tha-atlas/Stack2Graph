"""
Provide console, run-file, event, and progress logging.

Configure the console at an entry point, use get_logger(__name__) in libraries,
and wrap actual work in pipeline_run. Importing this module creates no handlers
or files. Context-local run IDs keep simultaneous notebook runs separate.
"""

from __future__ import annotations

import fcntl
import inspect
import io
import json
import logging
import os
import re
import sys
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import Field, asdict, dataclass, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Protocol, TextIO, TypeAlias, cast
from uuid import uuid4

from utils.types import JSONScalar

if TYPE_CHECKING:
    from rich.progress import Progress, TaskID


class DataclassEventValue(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Field[EventValue]]]


EventValue: TypeAlias = (
    JSONScalar | Path | datetime | DataclassEventValue | Sequence["EventValue"] | Mapping[str, "EventValue"]
)

_OWNED_CONSOLE_HANDLERS: set[logging.Handler] = set()

DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(stage)s | %(message)s"
FILE_LOG_FORMAT = "%(asctime)sZ | %(process)d | %(levelname)-7s | %(name)s | run=%(run_id)s | %(stage)s | %(message)s"
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07]*(?:\x07|\x1b\\)")
_ACTIVE_RUN: ContextVar[RunLog | None] = ContextVar("stack2graph_run", default=None)
_ACTIVE_STAGE: ContextVar[str | None] = ContextVar("stack2graph_stage", default=None)
_HIDE_CONSOLE_PROGRESS: ContextVar[bool] = ContextVar("stack2graph_hide_console_progress", default=False)
_PROGRESS_LOG_FORMAT = "%s: %s%s; %s/s; elapsed %s; ETA %s"


def get_logger(name: str, level: int | None = None) -> logging.Logger:
    """Return a library logger; destination handlers choose the displayed level."""
    logger = logging.getLogger(name)
    if level is not None or logger.level == logging.NOTSET:
        logger.setLevel(logging.DEBUG if level is None else level)
    return logger


class _ContextFilter(logging.Filter):
    def __init__(self, run_id: str | None = None) -> None:
        """Optionally restrict records to a single run ID."""
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        """Add run and stage fields to the record and check its run ID."""
        run = _ACTIVE_RUN.get()
        active_id = run.run_id if run else "-"
        record.run_id = active_id
        record.stage = _ACTIVE_STAGE.get() or (run.name if run else "app")
        return self.run_id is None or active_id == self.run_id


class _PlainFormatter(logging.Formatter):
    def converter(self, timestamp: float | None = None) -> time.struct_time:
        """Convert a timestamp to UTC for log formatting."""
        return time.gmtime(timestamp)

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record without ANSI escapes or carriage returns."""
        return _ANSI_ESCAPE.sub("", super().format(record)).replace("\r", "")


class _ProcessFileHandler(logging.FileHandler):
    """Write complete log records from workers sharing the same run file."""

    def __init__(self, filename: Path, *, encoding: str) -> None:
        """Open the log file and retain its descriptor for process locking."""
        super().__init__(filename, encoding=encoding)
        assert self.stream is not None
        self._descriptor = self.stream.fileno()

    def emit(self, record: logging.LogRecord) -> None:
        """Write a log record while holding an exclusive file lock."""
        if self.stream is None:
            return
        try:
            fcntl.flock(self._descriptor, fcntl.LOCK_EX)
            try:
                super().emit(record)
            finally:
                fcntl.flock(self._descriptor, fcntl.LOCK_UN)
        except OSError:
            self.handleError(record)


class _ProgressConsoleFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        """Allow console records unless progress output is hidden."""
        del record
        return not _HIDE_CONSOLE_PROGRESS.get()


class _ConsoleLoggerLevelFilter(logging.Filter):
    def __init__(self, levels: Mapping[str, int]) -> None:
        """Copy the minimum console levels for each logger namespace."""
        super().__init__()
        self.levels = dict(levels)

    def filter(self, record: logging.LogRecord) -> bool:
        """Check a record against all matching namespace level limits."""
        return all(
            record.levelno >= level
            for name, level in self.levels.items()
            if record.name == name or record.name.startswith(name + ".")
        )


def setup_logging(
    level: int = DEFAULT_LOG_LEVEL,
    *,
    stream: TextIO | None = None,
    force: bool = False,
    console_logger_levels: Mapping[str, int] | None = None,
) -> None:
    """
    Configure one stderr console handler while preserving host-owned handlers.

    Rich is loaded only for an interactive terminal. Redirected output,
    NO_COLOR, and environments without Rich use readable plain lines.
    Reconfiguration removes and closes only the console handler owned by this
    helper. Optional namespace levels quiet console chatter while preserving run
    files.
    """
    root = logging.getLogger()
    owned = [handler for handler in root.handlers if handler in _OWNED_CONSOLE_HANDLERS]
    if owned and not force:
        owned[0].setLevel(level)
        for console_filter in owned[0].filters:
            if isinstance(console_filter, _ConsoleLoggerLevelFilter):
                console_filter.levels = dict(console_logger_levels or {})
        root.setLevel(level)
        return
    for owned_handler in owned:
        root.removeHandler(owned_handler)
        owned_handler.close()
        _OWNED_CONSOLE_HANDLERS.discard(owned_handler)
    output = sys.stderr if stream is None else stream
    interactive = cast(bool, output.isatty()) and "NO_COLOR" not in os.environ
    handler: logging.Handler | None = None
    if interactive:
        try:
            from rich.console import Console
            from rich.logging import RichHandler

            handler = RichHandler(
                console=Console(file=output, markup=False),
                show_path=False,
                markup=False,
                log_time_format="%H:%M:%S",
                omit_repeated_times=False,
            )
            handler.setFormatter(_PlainFormatter("%(stage)s | %(message)s"))
        except ImportError:
            pass
    if handler is None:
        handler = logging.StreamHandler(output)
        handler.setFormatter(_PlainFormatter(DEFAULT_LOG_FORMAT, datefmt="%H:%M:%S"))
    handler.setLevel(level)
    handler.addFilter(_ContextFilter())
    handler.addFilter(_ProgressConsoleFilter())
    handler.addFilter(_ConsoleLoggerLevelFilter(console_logger_levels or {}))
    _OWNED_CONSOLE_HANDLERS.add(handler)
    root.addHandler(handler)
    root.setLevel(level)


def _json_default(value: EventValue) -> EventValue:
    """Convert event dataclasses and timestamps to JSON-compatible values."""
    if not inspect.isclass(value) and is_dataclass(value):
        return cast(dict[str, EventValue], asdict(value))
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def append_run_event(log_path: str | Path, event: str, **fields: EventValue) -> None:
    """Append one locked JSONL event; logging I/O must not abort processing."""
    run = _ACTIVE_RUN.get()
    payload = {
        **fields,
        "timestamp": datetime.now(UTC).isoformat(),
        "event": event,
        "pid": os.getpid(),
        "run_id": run.run_id if run else None,
        "stage": _ACTIVE_STAGE.get() or (run.name if run else None),
    }
    try:
        path = Path(log_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(payload, ensure_ascii=True, sort_keys=True, default=_json_default)
        with path.open("a", encoding="utf-8") as file:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX)
            try:
                file.write(line + "\n")
                file.flush()
            finally:
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)
    except OSError:
        # Event output is diagnostic; committed data and manifests remain authoritative.
        return


def stage_event(event: str, **fields: EventValue) -> None:
    """Record an event in the current run, or do nothing outside a run."""
    run = _ACTIVE_RUN.get()
    if run is not None:
        append_run_event(run.events_file, event, **fields)


def format_elapsed(seconds: float) -> str:
    """Format rounded seconds as hours, minutes, and seconds."""
    minutes, remaining_seconds = divmod(round(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m {remaining_seconds:02d}s" if hours else f"{minutes}m {remaining_seconds:02d}s"


def format_bytes(size: float) -> str:
    """Format a byte count using binary units up to TiB."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(size) < 1024 or unit == "TiB":
            return f"{size:,.1f} {unit}"
        size /= 1024
    raise AssertionError("Unreachable byte unit")


@dataclass(frozen=True, slots=True)
class RunLog:
    name: str
    run_id: str
    log_file: Path | None
    events_file: Path
    logger: logging.Logger

    def event(self, name: str, **fields: EventValue) -> None:
        """Append an event with this run's name as its prefix."""
        append_run_event(self.events_file, f"{self.name}_{name}", **fields)

    def summary(self, title: str, rows: Mapping[str, EventValue]) -> None:
        """Render a small result table as plain text; handlers add terminal styling."""
        try:
            from rich.console import Console
            from rich.table import Table
            from rich.text import Text

            table = Table(title=Text(title), show_header=True, header_style="bold cyan")
            table.add_column("Metric")
            table.add_column("Value", justify="right")
            for name, value in rows.items():
                formatted = f"{value:,}" if isinstance(value, int) and not isinstance(value, bool) else str(value)
                table.add_row(Text(str(name)), Text(formatted))
            output = io.StringIO()
            Console(file=output, width=110, force_terminal=False, color_system=None).print(table)
            self.logger.info("\n%s", output.getvalue().rstrip())
        except ImportError:
            self.logger.info("%s: %s", title, "; ".join(f"{key}={value}" for key, value in rows.items()))

    @contextmanager
    def stage(self, name: str) -> Iterator[None]:
        """Track one stage without opening another run file or resetting its ID."""
        token = _ACTIVE_STAGE.set(name)
        started = time.perf_counter()
        self.logger.info("%s started", name)
        append_run_event(self.events_file, f"{name}_started")
        try:
            yield
        except BaseException as error:
            status = "cancelled" if isinstance(error, KeyboardInterrupt) else "failed"
            append_run_event(
                self.events_file,
                f"{name}_{status}",
                elapsed_seconds=time.perf_counter() - started,
                error_type=type(error).__name__,
                error_message=str(error),
            )
            raise
        else:
            elapsed = time.perf_counter() - started
            append_run_event(self.events_file, f"{name}_completed", elapsed_seconds=elapsed)
            self.logger.info("%s finished in %s", name, format_elapsed(elapsed))
        finally:
            _ACTIVE_STAGE.reset(token)


@contextmanager
def pipeline_run(
    stage: str,
    *,
    log_dir: str | Path,
    settings: Mapping[str, EventValue] | None = None,
    run_logger: logging.Logger | None = None,
    log_name: str | None = None,
    reuse_active: bool = False,
) -> Iterator[RunLog]:
    """
    Attach scoped file logging and record the run outcome.

    Console configuration remains the entry point's responsibility. Libraries
    and notebooks can use this context without replacing their existing
    handlers. File output accepts DEBUG records from application loggers. Every
    owned file handler closes on exit; context tokens restore an enclosing run.
    With reuse_active, nested operations become stages of the existing run.
    """
    active = _ACTIVE_RUN.get()
    if reuse_active and active is not None:
        with active.stage(stage):
            yield active
        return
    directory = Path(log_dir)
    active_logger = run_logger or get_logger(stage)
    run_id = uuid4().hex
    file_handler = None
    requested_log_file = directory / (log_name or f"{stage}.log")
    log_file: Path | None = requested_log_file
    try:
        directory.mkdir(parents=True, exist_ok=True)
        file_handler = _ProcessFileHandler(requested_log_file, encoding="utf-8")
        file_handler.setFormatter(_PlainFormatter(FILE_LOG_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S"))
        file_handler.addFilter(_ContextFilter(run_id))
        logging.getLogger().addHandler(file_handler)
    except OSError as error:
        log_file = None
        active_logger.warning("Run file logging unavailable: %s", error)
    run = RunLog(stage, run_id, log_file, directory / "run_events.jsonl", active_logger)
    run_token = _ACTIVE_RUN.set(run)
    stage_token = _ACTIVE_STAGE.set(stage)
    started = time.perf_counter()
    try:
        active_logger.info("%s started (run %s); log: %s", stage, run_id[:8], log_file or "unavailable")
        if settings:
            run.summary(f"{stage} settings", settings)
        run.event("started", **({"settings": dict(settings)} if settings else {}))
        try:
            yield run
        except BaseException as error:
            elapsed = time.perf_counter() - started
            if isinstance(error, SystemExit) and error.code in (0, None):
                run.event("completed", elapsed_seconds=elapsed)
                active_logger.info("%s finished in %s", stage, format_elapsed(elapsed))
            elif isinstance(error, KeyboardInterrupt):
                run.event("cancelled", elapsed_seconds=elapsed)
                active_logger.warning(
                    "%s cancelled after %s; rerun the stage explicitly or use run --overwrite",
                    stage,
                    format_elapsed(elapsed),
                )
            else:
                run.event("failed", elapsed_seconds=elapsed, error_type=type(error).__name__, error_message=str(error))
                active_logger.error(
                    "%s failed after %s: %s: %s",
                    stage,
                    format_elapsed(elapsed),
                    type(error).__name__,
                    error,
                )
                active_logger.debug("Failure traceback", exc_info=True)
            raise
        else:
            elapsed = time.perf_counter() - started
            run.event("completed", elapsed_seconds=elapsed)
            active_logger.info("%s finished in %s", stage, format_elapsed(elapsed))
    finally:
        try:
            if file_handler is not None:
                logging.getLogger().removeHandler(file_handler)
                try:
                    file_handler.close()
                except OSError, ValueError:
                    # A diagnostic flush failure must not replace the processing result.
                    pass
        finally:
            _ACTIVE_STAGE.reset(stage_token)
            _ACTIVE_RUN.reset(run_token)


def current_run() -> RunLog | None:
    """Capture the explicit, pickleable run context before spawning workers."""
    return _ACTIVE_RUN.get()


@contextmanager
def worker_logging(run: RunLog | None) -> Iterator[None]:
    """Attach a parent's run in a spawned worker without starting a second run."""
    if run is None or _ACTIVE_RUN.get() is run:
        yield
        return
    get_logger(run.logger.name)
    handler = None
    if run.log_file is not None:
        try:
            handler = _ProcessFileHandler(run.log_file, encoding="utf-8")
            handler.setFormatter(_PlainFormatter(FILE_LOG_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S"))
            handler.addFilter(_ContextFilter(run.run_id))
            logging.getLogger().addHandler(handler)
        except OSError as error:
            run.logger.warning("Worker file logging unavailable: %s", error)
    run_token = _ACTIVE_RUN.set(run)
    stage_token = _ACTIVE_STAGE.set(run.name)
    try:
        yield
    finally:
        try:
            if handler is not None:
                logging.getLogger().removeHandler(handler)
                try:
                    handler.close()
                except OSError, ValueError:
                    pass
        finally:
            _ACTIVE_STAGE.reset(stage_token)
            _ACTIVE_RUN.reset(run_token)


class ProgressReporter:
    """Display task progress and emit interval-limited logs with time estimates."""

    def __init__(
        self,
        logger: logging.Logger,
        label: str,
        *,
        unit: str = "bytes",
        interval: float = 10,
        initial: int = 0,
        total: int | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Set up progress counters, reporting intervals, and the rate clock."""
        if interval <= 0:
            raise ValueError("Progress interval must be positive")
        self.logger, self.label, self.unit = logger, label, unit
        self.interval, self.initial, self.clock = interval, initial, clock
        self.total = total
        self.started = self.last_report = clock()
        self.last_display = self.started
        self.display: Progress | None = None
        self.task: TaskID | None = None

    @contextmanager
    def track(self, *, shared_display: Progress | None = None) -> Iterator[ProgressReporter]:
        """
        Track a task in an owned or borrowed display and restore its cursor.

        Borrowed tasks remain visible for their display owner's final render.
        """
        if shared_display is not None:
            self.display = shared_display
            self.task = shared_display.add_task(self.label, total=self.total, completed=self.initial, status="starting")
            try:
                self.update(self.initial, self.total)
                yield self
            finally:
                self.display = None
                self.task = None
            return
        try:
            from rich.logging import RichHandler
            from rich.progress import BarColumn, Progress, TextColumn
            from rich.table import Column
        except ImportError:
            yield self
            return
        handler = next(
            (
                item
                for item in logging.getLogger().handlers
                if item in _OWNED_CONSOLE_HANDLERS and isinstance(item, RichHandler)
            ),
            None,
        )
        if handler is None or handler.level > logging.INFO or not self.logger.isEnabledFor(logging.INFO):
            yield self
            return
        with (
            Progress(
                TextColumn("{task.description}", markup=False),
                BarColumn(bar_width=20),
                TextColumn("{task.fields[status]}", markup=False, table_column=Column(no_wrap=False)),
                console=handler.console,
                auto_refresh=False,
            ) as display,
            self.track(shared_display=display),
        ):
            yield self

    def restart(self, *, initial: int = 0) -> None:
        """Reset the rate clock and baseline for fresh or resumed work."""
        self.initial = initial
        self.started = self.last_report = self.clock()
        self.last_display = self.started
        self.update(initial, self.total)

    def update(self, completed: int, total: int | None = None, *, force: bool = False, refresh: bool = False) -> None:
        """Update counters; refresh redraws immediately without forcing a log line."""
        now = self.clock()
        if total is not None:
            self.total = total
        total = self.total
        elapsed = max(now - self.started, 0.001)
        rate = max(0, completed - self.initial) / elapsed
        remaining = max(0, total - completed) if total is not None else None
        eta = 0.0 if remaining == 0 else remaining / rate if remaining is not None and rate > 0 else None

        def format_units(value: float) -> str:
            """Format a progress quantity with the configured unit."""
            return f"{value:,.0f} {self.unit}"

        formatter = format_bytes if self.unit == "bytes" else format_units
        rate_text = format_bytes(rate) if self.unit == "bytes" else f"{rate:,.2f} {self.unit}"
        fraction = f" / {formatter(total)} ({completed / total:.1%})" if total else ""
        eta_text = format_elapsed(eta) if eta is not None else "unknown"
        if (
            self.display is not None
            and self.task is not None
            and (force or refresh or now - self.last_display >= 0.2 or completed == self.initial)
        ):
            self.last_display = now
            self.display.update(
                self.task,
                completed=completed,
                total=total,
                status=f"{formatter(completed)}{fraction}; {rate_text}/s; elapsed {format_elapsed(elapsed)}; ETA {eta_text}",
                refresh=True,
            )
        if not force and now - self.last_report < self.interval:
            return
        self.last_report = now
        token = _HIDE_CONSOLE_PROGRESS.set(self.display is not None)
        try:
            self.logger.info(
                _PROGRESS_LOG_FORMAT,
                self.label,
                formatter(completed),
                fraction,
                rate_text,
                format_elapsed(elapsed),
                eta_text,
            )
        finally:
            _HIDE_CONSOLE_PROGRESS.reset(token)
        stage_event(
            "progress",
            operation=self.label,
            completed=completed,
            total=total,
            unit=self.unit,
            rate_per_second=round(rate, 3),
            elapsed_seconds=round(elapsed, 3),
            remaining_seconds=round(eta, 3) if eta is not None else None,
        )
