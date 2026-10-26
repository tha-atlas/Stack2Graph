"""Define the source operations used by graph processing services."""

from collections.abc import Iterable, Iterator
from typing import Protocol

from graph.domain.events import QuestionEvent, SharedEvent
from graph.domain.source import DatabaseCapacity, QuestionRange, SourceInventory, TagUsage
from utils.resources import BatchLimits


class GraphSource(Protocol):
    def describe(self) -> SourceInventory:
        """Read the pinned source inventory and exclusion evidence."""
        ...

    def database_capacity(self) -> DatabaseCapacity:
        """Read available database capacity for export workers."""
        ...

    def iter_questions(self, part: QuestionRange, limits: BatchLimits) -> Iterator[QuestionEvent]:
        """Yield question events within the requested range and batch limits."""
        ...

    def iter_shared(self, usages: Iterable[TagUsage], limits: BatchLimits) -> Iterator[SharedEvent]:
        """Yield shared content events for the supplied tag usages."""
        ...

    def close(self) -> None:
        """Release the source's database resources."""
        ...
