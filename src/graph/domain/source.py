"""Preserve source records and define fixed question-ID ranges."""

import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime

from sql.domain.models import SnapshotIdentity

IMPORTED_TABLES: tuple[str, ...] = ("Posts", "Comments", "Tags", "Votes", "PostLinks")


@dataclass(frozen=True)
class PostDates:
    creation: str | None = None
    last_edit: str | None = None
    last_activity: str | None = None
    closed: str | None = None
    community_owned: str | None = None


@dataclass(frozen=True)
class PostRecord:
    id: int
    post_type: int
    parent_id: int | None = None
    title: str | None = None
    body_html: str | None = None
    tags: tuple[str, ...] = ()
    score: int | None = None
    view_count: int | None = None
    answer_count: int | None = None
    comment_count: int | None = None
    favorite_count: int | None = None
    accepted_answer_id: int | None = None
    dates: PostDates = PostDates()
    license: str | None = None


@dataclass(frozen=True)
class CommentRecord:
    id: int
    post_id: int
    text: str | None
    score: int | None
    creation: str | None
    license: str | None


@dataclass(frozen=True)
class TagRecord:
    id: int
    name: str
    question_count: int | None
    wiki_id: int | None
    excerpt_id: int | None


@dataclass(frozen=True)
class VoteCounts:
    up: int
    down: int


@dataclass(frozen=True)
class PostReference:
    id: int
    exists: bool
    post_type: int | None
    parent_id: int | None


def resolved_accepted_answer(reference: PostReference, question_id: int) -> bool:
    """Return whether a declared accepted answer can safely become a graph edge."""
    return reference.exists and reference.post_type == 2 and reference.parent_id == question_id


def contradicts_observed_answers(answer_count: int | None, observed_answers: int) -> bool:
    """Check whether AnswerCount contradicts the observed answer rows."""
    return answer_count is not None and (answer_count < 0 or (answer_count == 0 and observed_answers > 0))


@dataclass(frozen=True)
class SourceProblem:
    code: str
    table: str
    source_id: int
    field: str
    target_id: int | None
    message: str


class SourceIntegrityError(ValueError):
    def __init__(self, problems: tuple[SourceProblem, ...]) -> None:
        """Retain diagnostic source problems and build a readable error message."""
        if not problems:
            raise ValueError("Source integrity failure requires diagnostic evidence")
        self.problems = problems
        super().__init__("; ".join(f"{p.table}[{p.source_id}].{p.field}: {p.message}" for p in problems))

    def __reduce__(self) -> tuple[type[SourceIntegrityError], tuple[tuple[SourceProblem, ...]]]:
        """Preserve structured source problems when sent across processes."""
        return type(self), (self.problems,)


@dataclass(frozen=True, order=True)
class QuestionRange:
    first_id: int
    end_id: int

    def __post_init__(self) -> None:
        """Require positive half-open bounds with a nonempty interval."""
        if self.first_id < 1 or self.end_id <= self.first_id:
            raise ValueError("Question range must have positive half-open boundaries")

    @property
    def key(self) -> str:
        """The padded filename key for this half-open question range."""
        return f"{self.first_id:020d}-{self.end_id:020d}"


@dataclass(frozen=True, order=True)
class TagUsage:
    tag_id: int
    graph_key: str


@dataclass(frozen=True)
class SourceInventory:
    snapshot: SnapshotIdentity
    table_counts: tuple[tuple[str, int], ...]
    question_count: int
    first_question_id: int | None
    last_question_id: int | None
    exclusions: tuple[tuple[str, int], ...]
    orphan_documentation: int = 0


@dataclass(frozen=True)
class DatabaseCapacity:
    connection_slots: int


def question_ranges(inventory: SourceInventory, width: int) -> Iterator[QuestionRange]:
    """Yield fixed-width half-open ranges covering the inventory's question IDs."""
    if width < 1:
        raise ValueError("Range width must be positive")
    first, last = inventory.first_question_id, inventory.last_question_id
    if first is not None and last is not None:
        for start in range(first, last + 1, width):
            yield QuestionRange(start, min(start + width, last + 1))


def canonical_datetime(value: str) -> str:
    """Validate a source timestamp, trim to milliseconds, and default to UTC."""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?", value) is None:
        raise ValueError(f"Invalid source dateTime: {value!r}")
    datetime.fromisoformat(value)

    def milliseconds(match: re.Match[str]) -> str:
        """Keep up to three fractional digits and remove trailing zeros."""
        digits: str = match.group(1)
        fraction = digits[:3].rstrip("0")
        return "." + fraction if fraction else ""

    value = re.sub(r"\.(\d+)", milliseconds, value)
    return value if value.endswith("Z") or re.search(r"[+-]\d{2}:\d{2}$", value) else value + "Z"
