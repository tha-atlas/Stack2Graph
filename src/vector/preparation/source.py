"""Decode published SQL question rows for vector extraction."""

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Protocol

from selection.tags import parse_tags
from sql.domain.models import Row
from sql.domain.rows import integer, optional_integer, optional_text, text
from sql.domain.schema import timestamp_ns
from vector.domain.models import QuestionRecord


def vector_question(row: Row) -> QuestionRecord:
    """Decode the narrow question projection without inventing quality values."""

    def optional_date(name: str) -> datetime | None:
        """Decode an optional source timestamp and attach UTC."""
        if row.get(name) is None:
            return None
        value = text(row, name)
        timestamp_ns(value)
        result = datetime.fromisoformat(value)
        return result.replace(tzinfo=UTC)

    tags = parse_tags(None if row.get("Tags") is None else text(row, "Tags"))
    return QuestionRecord(
        integer(row, "Id"),
        optional_text(row, "Title") or "",
        optional_text(row, "Body") or "",
        tags,
        optional_integer(row, "Score"),
        optional_integer(row, "AnswerCount"),
        optional_integer(row, "AcceptedAnswerId"),
        optional_date("CreationDate"),
        optional_date("LastEditDate"),
        optional_date("LastActivityDate"),
        optional_date("ClosedDate"),
    )


class QuestionRows(Protocol):
    def iter_question_rows(self, batch_rows: int = 1024, *, after_id: int = ...) -> Iterator[tuple[Row, ...]]:
        """Yield projected question rows in batches after an exclusive ID bound."""
        ...


class VectorQuestionSource:
    """Decode SQL question rows within the reader's page and byte limits."""

    def __init__(self, reader: QuestionRows) -> None:
        """Store the reader that supplies projected question rows."""
        self.reader = reader

    def iter_vector_questions(
        self, batch_rows: int = 1024, *, after_id: int = -(1 << 63)
    ) -> Iterator[tuple[QuestionRecord, ...]]:
        """Decode source rows into typed question batches."""
        for batch in self.reader.iter_question_rows(batch_rows, after_id=after_id):
            yield tuple(vector_question(row) for row in batch)
