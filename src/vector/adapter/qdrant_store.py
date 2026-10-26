"""Manage Qdrant rebuilds, acknowledged writes, and readiness checks."""

import hashlib
import time
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from typing import Protocol
from uuid import uuid4

from qdrant_client import QdrantClient, models

from utils.types import JSONObject, canonical_hash, json_object, parse_json
from vector.adapter.qdrant_client import create_qdrant_client
from vector.domain.config import QdrantConfig
from vector.domain.models import CollectionSchema, CollectionState, VectorPoint


def _point_payload(point: VectorPoint) -> JSONObject:
    """Copy point metadata, retaining optional fields present in the payload."""
    source = point.payload
    payload: JSONObject = {
        "question_id": source["question_id"],
        "snapshot_id": source["snapshot_id"],
        "chunk_index": source["chunk_index"],
        "chunk_count": source["chunk_count"],
        "tags": list(source["tags"]),
        "languages": list(source["languages"]),
    }
    optional: JSONObject = {
        "score": source.get("score"),
        "answer_count": source.get("answer_count"),
        "accepted_answer_id": source.get("accepted_answer_id"),
        "has_accepted_answer": source.get("has_accepted_answer"),
        "is_closed": source.get("is_closed"),
        "creation_date": source.get("creation_date"),
        "last_edit_date": source.get("last_edit_date"),
        "last_activity_date": source.get("last_activity_date"),
        "closed_date": source.get("closed_date"),
    }
    payload.update({key: value for key, value in optional.items() if key in source})
    return payload


def _point_struct(point: VectorPoint) -> models.PointStruct:
    """Build the SDK value without serializing vectors through JSON."""
    return models.PointStruct(
        id=str(point.point_id),
        vector={
            "dense": list(point.dense),
            "sparse": models.SparseVector(indices=list(point.sparse.indices), values=list(point.sparse.values)),
        },
        payload=_point_payload(point),
    )


class QdrantVectorStore:
    def __init__(
        self,
        config: QdrantConfig,
        *,
        client: _CollectionClient | None = None,
        readiness_client_factory: Callable[[int], _ReadinessClient] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Connect the store or use the supplied client and readiness hooks."""
        self.config = config
        self._monotonic, self._sleep = monotonic, sleep
        self._readiness_client_factory = readiness_client_factory or self._new_client
        self.client: _CollectionClient = (
            client if client is not None else create_qdrant_client(config, config.request_timeout)
        )

    def inspect(self) -> CollectionState | None:
        """Read collection state and exact point count, or None if absent."""
        if not self.client.collection_exists(self.config.collection):
            return None
        info = self.client.get_collection(self.config.collection)
        count = self.client.count(self.config.collection, exact=True).count
        return self._state(info, count)

    def replace(self, expected: CollectionState | None, schema: CollectionSchema) -> None:
        """Recreate the collection only if its state still matches preflight."""
        if self.inspect() != expected:
            raise RuntimeError("Qdrant collection changed since preflight; rebuild aborted")
        if expected is not None and not self.client.delete_collection(self.config.collection):
            raise RuntimeError("Qdrant did not acknowledge collection deletion")
        # Fresh metadata distinguishes recreation even when schema and count are identical.
        metadata: dict[str, str] = {
            "build_generation": schema.build_id if schema.build_id is not None else str(uuid4())
        }
        if not self.client.create_collection(
            self.config.collection,
            vectors_config={
                "dense": models.VectorParams(
                    size=schema.dense_dimension, distance=models.Distance.COSINE, memory=models.Memory.COLD
                )
            },
            sparse_vectors_config={"sparse": models.SparseVectorParams(index=models.SparseIndexParams(on_disk=True))},
            optimizers_config=models.OptimizersConfigDiff(indexing_threshold=0),
            shard_number=self.config.shard_number,
            metadata=metadata,
        ):
            raise RuntimeError("Qdrant did not acknowledge collection creation")
        indexes = {
            "question_id": models.PayloadSchemaType.INTEGER,
            "snapshot_id": models.PayloadSchemaType.KEYWORD,
            "tags": models.PayloadSchemaType.KEYWORD,
            "languages": models.PayloadSchemaType.KEYWORD,
            "score": models.PayloadSchemaType.INTEGER,
            "answer_count": models.PayloadSchemaType.INTEGER,
            "accepted_answer_id": models.PayloadSchemaType.INTEGER,
            "has_accepted_answer": models.PayloadSchemaType.BOOL,
            "is_closed": models.PayloadSchemaType.BOOL,
            "creation_date": models.PayloadSchemaType.DATETIME,
            "last_edit_date": models.PayloadSchemaType.DATETIME,
            "last_activity_date": models.PayloadSchemaType.DATETIME,
            "closed_date": models.PayloadSchemaType.DATETIME,
        }
        for name, schema_type in indexes.items():
            self.client.create_payload_index(self.config.collection, name, field_schema=schema_type, wait=True)

    def upload(self, points: Sequence[VectorPoint]) -> None:
        """Upsert a point batch and require Qdrant to confirm completion."""
        structures = [_point_struct(point) for point in points]
        result = self.client.upsert(self.config.collection, structures, wait=True)
        if result.status != models.UpdateStatus.COMPLETED:
            raise RuntimeError("Qdrant write was not completed")

    def _new_client(self, timeout: int) -> QdrantClient:
        """Create a timeout-specific client without a compatibility check."""
        return create_qdrant_client(self.config, timeout, check_compatibility=False)

    @contextmanager
    def _readiness_request(self, deadline: float) -> Generator[_ReadinessClient]:
        # The SDK's read methods ignore timeout kwargs. Configure a short-lived
        # client per request instead, without changing concurrent upload clients.
        # Its supported timeout type is whole seconds: do not start a request
        # when less than one second remains.
        """Provide a short-lived readiness client within a monotonic deadline."""
        timeout = min(self.config.request_timeout, int(deadline - self._monotonic()))
        if timeout <= 0:
            raise TimeoutError("Qdrant collection did not become ready before its finalization deadline")
        client = self._readiness_client_factory(timeout)
        try:
            if self._monotonic() >= deadline:
                raise TimeoutError("Qdrant finalization deadline expired during client creation")
            yield client
        finally:
            client.close()
        if self._monotonic() >= deadline:
            raise TimeoutError("Qdrant finalization deadline expired during a request or client cleanup")

    def _state(self, info: models.CollectionInfo, count: int) -> CollectionState:
        """Decode collection identity and configuration hashes for resume checks."""
        config_json = info.config.model_dump_json()
        digest = hashlib.sha256(config_json.encode()).hexdigest()
        config = json_object(parse_json(config_json))
        metadata = config.get("metadata")
        generation = metadata.get("build_generation") if isinstance(metadata, dict) else None
        build_id = generation if isinstance(generation, str) else None
        params = config.get("params")
        vectors = params.get("vectors") if isinstance(params, dict) else None
        dense = vectors.get("dense") if isinstance(vectors, dict) else None
        size = dense.get("size") if isinstance(dense, dict) else None
        dimension = size if type(size) is int and size > 0 else None
        optimizer = config.get("optimizer_config")
        if isinstance(optimizer, dict):
            # Finalization changes this field; every other configuration field
            # remains part of the ownership guard for a resumed build.
            optimizer.pop("indexing_threshold", None)
        resume_digest = canonical_hash(config, allow_nan=False)
        return CollectionState(self.config.collection, count, digest, build_id, dimension, resume_digest=resume_digest)

    def _verify_count(self, expected_points: int, deadline: float) -> int:
        """Read an exact point count and reject a mismatch before the deadline."""
        with self._readiness_request(deadline) as client:
            count = client.count(self.config.collection, exact=True).count
        if count != expected_points:
            raise RuntimeError("Qdrant point count differs from confirmed build count")
        return count

    def verify(self, expected_points: int, *, deadline: float) -> CollectionState:
        """
        Restore indexing and wait for readiness and exact count verification.

        Args:
            expected_points: Confirmed point count for this build.
            deadline: Absolute deadline from the store's monotonic clock.

        Returns:
            Ready collection state after restoring the configured indexing policy.

        Raises:
            TimeoutError: Requests or optimization polling exceed the deadline.
            RuntimeError: Counts differ, indexing is unacknowledged, or optimization fails.

        """
        self._verify_count(expected_points, deadline)
        with self._readiness_request(deadline) as client:
            acknowledged = client.update_collection(
                self.config.collection,
                optimizers_config=models.OptimizersConfigDiff(indexing_threshold=self.config.indexing_threshold_kb),
            )
        if not acknowledged:
            raise RuntimeError("Qdrant did not acknowledge indexing policy restoration")
        quiet_polls = 0
        previous: tuple[int | None, int | None, int, int] | None = None
        while True:
            with self._readiness_request(deadline) as client:
                info = client.get_collection(self.config.collection)
            if info.optimizer_status != models.OptimizersStatusOneOf.OK:
                raise RuntimeError("Qdrant optimizer reported an error")
            with self._readiness_request(deadline) as client:
                progress = client.get_optimizations(self.config.collection)
            for optimization in (*progress.running, *(progress.completed or [])):
                if isinstance(optimization.status, models.TrackerStatusOneOf2):
                    raise RuntimeError("Qdrant optimizer reported an error")
            summary = progress.summary
            state = (info.points_count, info.indexed_vectors_count, info.segments_count, summary.idle_segments)
            quiet = (
                info.status == models.CollectionStatus.GREEN
                and info.config.optimizer_config.indexing_threshold == self.config.indexing_threshold_kb
                and not progress.running
                and not progress.queued
                and summary.queued_optimizations == summary.queued_segments == summary.queued_points == 0
            )
            quiet_polls = quiet_polls + 1 if quiet and state == previous else int(quiet)
            previous = state if quiet else None
            if quiet_polls >= 2:
                # The exact count and the ready-state configuration are the evidence the
                # publication gate consumes; both come from this final observation.
                count = self._verify_count(expected_points, deadline)
                with self._readiness_request(deadline) as client:
                    ready = client.get_collection(self.config.collection)
                return self._state(ready, count)
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise TimeoutError("Qdrant collection did not become ready")
            self._sleep(min(1.0, remaining))

    def close(self) -> None:
        """Close the store's Qdrant client."""
        self.client.close()


class _ReadinessClient(Protocol):
    """Define Qdrant requests used to check completion before a deadline."""

    def get_collection(self, collection_name: str) -> models.CollectionInfo:
        """Read the collection's configuration and status."""
        ...

    def get_optimizations(self, collection_name: str) -> models.OptimizationsResponse:
        """Read current and queued collection optimizations."""
        ...

    def count(self, collection_name: str, *, exact: bool) -> models.CountResult:
        """Count collection points with the requested accuracy."""
        ...

    def update_collection(self, collection_name: str, *, optimizers_config: models.OptimizersConfigDiff) -> bool:
        """Update optimizer settings and return the acknowledgement."""
        ...

    def close(self) -> None:
        """Release the readiness client's resources."""
        ...


class _CollectionClient(Protocol):
    """Define collection preflight, creation, and acknowledged write requests."""

    def collection_exists(self, collection_name: str) -> bool:
        """Return whether the named collection exists."""
        ...

    def get_collection(self, collection_name: str) -> models.CollectionInfo:
        """Read the collection's configuration and status."""
        ...

    def count(self, collection_name: str, *, exact: bool) -> models.CountResult:
        """Count collection points with the requested accuracy."""
        ...

    def delete_collection(self, collection_name: str) -> bool:
        """Delete the named collection and return the acknowledgement."""
        ...

    def create_collection(
        self,
        collection_name: str,
        *,
        vectors_config: Mapping[str, models.VectorParams],
        sparse_vectors_config: Mapping[str, models.SparseVectorParams],
        optimizers_config: models.OptimizersConfigDiff,
        shard_number: int,
        metadata: dict[str, str],
    ) -> bool:
        """Create a collection with named vectors, shards, and build metadata."""
        ...

    def create_payload_index(
        self,
        collection_name: str,
        field_name: str,
        *,
        field_schema: models.PayloadSchemaType,
        wait: bool,
    ) -> models.UpdateResult:
        """Create an index for one payload field with the requested schema."""
        ...

    def upsert(self, collection_name: str, points: Sequence[models.PointStruct], *, wait: bool) -> models.UpdateResult:
        """Insert or replace a point batch with optional completion waiting."""
        ...

    def close(self) -> None:
        """Release the collection client's resources."""
        ...
