"""Define worker and batch limits for vector uploads."""

from dataclasses import dataclass


@dataclass(frozen=True)
class LoadSettings:
    upload_points: int
    upload_workers: int
    attempts: int
