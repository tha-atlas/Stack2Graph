"""Display stage-marker progress and control dependency output."""

import logging
import os
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from threading import Event, Thread

from utils.logging_utils import ProgressReporter, get_logger
from vector.shards import RunLayout, StageMarker, read_json_file

LOGGER = get_logger(__name__)
DEPENDENCY_CONSOLE_LEVELS: dict[str, int] = dict.fromkeys(
    ("httpx", "httpcore", "sentence_transformers", "FlagEmbedding", "transformers", "huggingface_hub"),
    logging.WARNING,
)


def configure_embedding_output() -> None:
    """Keep dependency bars quiet in the controller and its spawned workers."""
    os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
    os.environ["TQDM_DISABLE"] = "1"
    # FlagEmbedding explicitly enables some bars, overriding TQDM_DISABLE.
    # It does not override delay; an infinite delay keeps those bars invisible.
    os.environ["TQDM_DELAY"] = "inf"


@dataclass(frozen=True)
class StageCounts:
    total_shards: int | None
    chunk_shards: int
    dense_shards: int
    sparse_shards: int
    loaded_shards: int
    loaded_points: int


def stage_counts(layout: RunLayout) -> StageCounts:
    """Count completed shard stages and confirmed loaded points."""
    shards = layout.finished_chunk_shards()
    loaded = [layout.loaded_done(shard) for shard in shards if layout.loaded_done(shard).exists()]
    return StageCounts(
        len(shards) if layout.extract_complete.exists() else None,
        len(shards),
        sum(layout.dense_done(shard).exists() for shard in shards),
        sum(layout.sparse_done(shard).exists() for shard in shards),
        len(loaded),
        sum(StageMarker.from_json(read_json_file(path)).rows for path in loaded),
    )


class RunProgress:
    """Refresh one progress line per stage from completion markers."""

    def __init__(self, layout: RunLayout, *, interval: float = 2.0) -> None:
        """Set up shard and point reporters with a shared polling interval."""
        self.layout, self.interval = layout, interval
        self.reporters: tuple[ProgressReporter, ...] = (
            ProgressReporter(LOGGER, "Chunk shards", unit="shards", interval=60),
            ProgressReporter(LOGGER, "Dense shards", unit="shards", interval=60),
            ProgressReporter(LOGGER, "Sparse shards", unit="shards", interval=60),
            ProgressReporter(LOGGER, "Loaded shards", unit="shards", interval=60),
            ProgressReporter(LOGGER, "Loaded points", unit="points", interval=60),
        )

    def refresh(self) -> None:
        """Read stage markers and update every progress reporter."""
        counts = stage_counts(self.layout)
        values = (counts.chunk_shards, counts.dense_shards, counts.sparse_shards, counts.loaded_shards)
        for reporter, value in zip(self.reporters[:4], values, strict=True):
            reporter.update(value, counts.total_shards)
        self.reporters[4].update(counts.loaded_points)

    @contextmanager
    def track(self) -> Generator[None]:
        """Provide shared progress displays and stop the polling thread on exit."""
        stop = Event()

        def poll() -> None:
            """Refresh progress until the stop event is set."""
            while not stop.wait(self.interval):
                self.refresh()

        with ExitStack() as stack:
            first = stack.enter_context(self.reporters[0].track())
            for reporter in self.reporters[1:]:
                stack.enter_context(reporter.track(shared_display=first.display))
            thread = Thread(target=poll, name="vector-progress", daemon=True)
            thread.start()
            try:
                yield
            finally:
                stop.set()
                thread.join()
                self.refresh()
