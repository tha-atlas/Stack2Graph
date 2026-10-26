"""Measure worker concurrency for XML preparation and database writes."""

from dataclasses import dataclass
from typing import Literal

from utils.logging_utils import format_bytes, get_logger

LOGGER = get_logger(__name__)


@dataclass
class ThroughputTuner:
    """Calibrate worker count once and keep the best measured total throughput."""

    maximum: int
    label: str = "XML"
    unit: Literal["rows", "bytes"] = "rows"
    width: int = 1
    best_width: int = 1
    best_rate: float = 0.0
    rows: int = 0
    seconds: float = 0.0
    blocks: int = 0
    warmup: int = 2
    settled: bool = False

    def observe(self, rows: int, seconds: float) -> None:
        """Record throughput and adjust worker count after warmup measurements."""
        if self.settled:
            return
        if self.warmup:
            self.warmup -= 1
            return
        self.rows += rows
        self.seconds += seconds
        self.blocks += 1
        if self.seconds < 2.0 or self.blocks < max(4, self.width * 2):
            return
        rate = self.rows / self.seconds
        tested_width = self.width
        if rate > self.best_rate * 1.05:
            self.best_rate, self.best_width = rate, self.width
            self.width = min(self.maximum, self.width * 2)
            self.settled = self.width == self.best_width
        else:
            self.width = self.best_width
            self.settled = True
        measured = format_bytes(int(rate)) if self.unit == "bytes" else f"{rate:.0f} rows"
        LOGGER.info(
            "%s calibration: tested %s workers; measured %s/s; %s %s workers (ceiling %s)",
            self.label,
            tested_width,
            measured,
            "selected" if self.settled else "next test",
            self.width,
            self.maximum,
        )
        self.rows, self.seconds, self.blocks, self.warmup = 0, 0.0, 0, self.width
