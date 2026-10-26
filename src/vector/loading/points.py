"""Build stable point IDs and metadata payloads that exclude source text."""

import json
from datetime import UTC, datetime
from typing import TypedDict
from uuid import NAMESPACE_URL, UUID, uuid5

from vector.domain.models import ChildChunk, EmbeddedChunk, PointPayload, VectorPoint


def _date(value: datetime | None) -> str | None:
    """Format a timestamp in UTC, rejecting naive timestamps."""
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("Question timestamps require a timezone")
    return value.astimezone(UTC).isoformat()


def make_payload(chunk: ChildChunk, snapshot_id: str) -> PointPayload:
    """Build point metadata from a child chunk and snapshot identifier."""
    question = chunk.selected.question
    return {
        "question_id": question.question_id,
        "snapshot_id": snapshot_id,
        "chunk_index": chunk.chunk_index,
        "chunk_count": chunk.chunk_count,
        "tags": list(question.tags),
        "languages": list(chunk.selected.languages),
        "score": question.score,
        "answer_count": question.answer_count,
        "accepted_answer_id": question.accepted_answer_id,
        "has_accepted_answer": question.accepted_answer_id is not None,
        "is_closed": question.closed_date is not None,
        "creation_date": _date(question.creation_date),
        "last_edit_date": _date(question.last_edit_date),
        "last_activity_date": _date(question.last_activity_date),
        "closed_date": _date(question.closed_date),
    }


def point_id(snapshot_id: str, configuration_digest: str, question_id: int, chunk_index: int) -> UUID:
    """Derive a stable UUID from snapshot, configuration, question, and chunk."""
    return uuid5(NAMESPACE_URL, f"stack2graph/{snapshot_id}/{configuration_digest}/{question_id}/{chunk_index}")


def make_point(embedded: EmbeddedChunk, snapshot_id: str, configuration_digest: str) -> VectorPoint:
    """Combine an embedded chunk with its stable ID and metadata payload."""
    payload = make_payload(embedded.chunk, snapshot_id)
    identity = point_id(snapshot_id, configuration_digest, payload["question_id"], payload["chunk_index"])
    return VectorPoint(identity, embedded.dense, embedded.sparse, payload)


class SparseJSON(TypedDict):
    indices: list[int]
    values: list[float]


class VectorJSON(TypedDict):
    dense: list[float]
    sparse: SparseJSON


class PointJSON(TypedDict):
    id: str
    vector: VectorJSON
    payload: PointPayload


def serialize_point(point: VectorPoint) -> str:
    """Encode a vector point as compact JSON with named dense/sparse vectors."""
    value: PointJSON = {
        "id": str(point.point_id),
        "vector": {
            "dense": list(point.dense),
            "sparse": {"indices": list(point.sparse.indices), "values": list(point.sparse.values)},
        },
        "payload": point.payload,
    }
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
