"""Define records exchanged by preparation, inference, and storage."""

from dataclasses import dataclass
from datetime import datetime
from typing import NotRequired, TypedDict
from uuid import UUID


@dataclass(frozen=True)
class QuestionRecord:
    question_id: int
    title: str
    body_html: str
    tags: tuple[str, ...]
    score: int | None = None
    answer_count: int | None = None
    accepted_answer_id: int | None = None
    creation_date: datetime | None = None
    last_edit_date: datetime | None = None
    last_activity_date: datetime | None = None
    closed_date: datetime | None = None


@dataclass(frozen=True)
class SelectedQuestion:
    question: QuestionRecord
    languages: tuple[str, ...]


@dataclass(frozen=True)
class ChildChunk:
    selected: SelectedQuestion
    chunk_index: int
    chunk_count: int
    embedding_text: str
    body_text: str
    body_start: int
    body_end: int
    overlap_prefix_chars: int


@dataclass(frozen=True)
class SparseWeights:
    indices: tuple[int, ...]
    values: tuple[float, ...]


@dataclass(frozen=True)
class EmbeddedChunk:
    chunk: ChildChunk
    dense: tuple[float, ...]
    sparse: SparseWeights


@dataclass(frozen=True)
class PreparedChunk:
    """Keep a lossless child chunk and both validated model input lengths."""

    chunk: ChildChunk
    dense_tokens: int
    sparse_tokens: int


class PointPayload(TypedDict):
    question_id: int
    snapshot_id: str
    chunk_index: int
    chunk_count: int
    tags: list[str]
    languages: list[str]
    score: NotRequired[int | None]
    answer_count: NotRequired[int | None]
    accepted_answer_id: NotRequired[int | None]
    has_accepted_answer: NotRequired[bool]
    is_closed: NotRequired[bool]
    creation_date: NotRequired[str | None]
    last_edit_date: NotRequired[str | None]
    last_activity_date: NotRequired[str | None]
    closed_date: NotRequired[str | None]


@dataclass(frozen=True)
class VectorPoint:
    point_id: UUID
    dense: tuple[float, ...]
    sparse: SparseWeights
    payload: PointPayload


@dataclass(frozen=True)
class CollectionState:
    name: str
    point_count: int
    configuration_digest: str
    build_id: str | None = None
    dense_dimension: int | None = None
    resume_digest: str | None = None


@dataclass(frozen=True)
class CollectionSchema:
    dense_dimension: int
    build_id: str | None = None
