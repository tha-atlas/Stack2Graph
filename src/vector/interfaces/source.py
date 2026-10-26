"""Define question batches read during vector extraction."""

from collections.abc import Iterator
from typing import Protocol

from vector.domain.models import QuestionRecord


class QuestionBatches(Protocol):
    def iter_vector_questions(
        self, batch_rows: int = 1024, *, after_id: int = ...
    ) -> Iterator[tuple[QuestionRecord, ...]]:
        """Yield question batches after the given exclusive ID bound."""
        ...
