"""Validate configuration for SQL import and database connections."""

from dataclasses import dataclass, field

from utils.config import DataPaths
from utils.resources import ResourceCaps


@dataclass(frozen=True)
class PipelineConfig:
    paths: DataPaths
    caps: ResourceCaps = field(default_factory=ResourceCaps)
    xml_workers: int | None = None

    def __post_init__(self) -> None:
        """Require a positive XML worker count when explicitly configured."""
        if self.xml_workers is not None and self.xml_workers <= 0:
            raise ValueError("XML workers must be positive")
