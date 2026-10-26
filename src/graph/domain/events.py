"""Stream source events within budgets, including large question groups."""

from dataclasses import dataclass

from graph.domain.source import CommentRecord, PostRecord, PostReference, TagRecord, VoteCounts


@dataclass(frozen=True)
class QuestionStart:
    post: PostRecord
    tags: tuple[TagRecord, ...]
    votes: VoteCounts
    observed_answers: int
    accepted: PostReference | None
    graph_keys: tuple[str, ...]


@dataclass(frozen=True)
class PostEvent:
    post: PostRecord
    votes: VoteCounts


@dataclass(frozen=True)
class CommentEvent:
    comment: CommentRecord
    parent: PostReference


@dataclass(frozen=True)
class LinkEvent:
    source_id: int
    target: PostReference
    link_type: int
    source_rows: int


@dataclass(frozen=True)
class QuestionEnd:
    question_id: int


@dataclass(frozen=True)
class SharedStart:
    tag: TagRecord | None
    graphs: tuple[str, ...]


@dataclass(frozen=True)
class SharedEnd:
    pass


QuestionEvent = QuestionStart | PostEvent | CommentEvent | LinkEvent | QuestionEnd
SharedEvent = SharedStart | PostEvent | CommentEvent | SharedEnd
