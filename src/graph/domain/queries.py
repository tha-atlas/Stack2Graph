"""Define typed results for reusable graph queries."""

from dataclasses import dataclass


@dataclass(frozen=True)
class AnswerDetails:
    iri: str
    body: str | None
    score: int | None


@dataclass(frozen=True)
class AnswerComment:
    answer_body: str | None
    text: str | None
    score: int | None


@dataclass(frozen=True)
class TagDocumentation:
    name: str
    source_id: int
    wiki_body: str | None
    excerpt_body: str | None


@dataclass(frozen=True)
class QuestionCounts:
    score: int | None
    views: int | None
    answers: int | None
    comments: int | None
