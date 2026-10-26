"""Read live Qdrant identity, configuration, and exact point counts."""

from typing import Protocol

from qdrant_client import models

from utils.types import JSONObject, JSONValue, json_integer, json_object, json_string, parse_json
from validation.domain.models import QdrantCollectionEvidence
from vector.adapter.qdrant_client import create_qdrant_client
from vector.domain.config import QdrantConfig


class _QdrantReader(Protocol):
    """Define synchronous Qdrant reads used by validation."""

    def collection_exists(self, collection_name: str) -> bool:
        """Return whether the named collection exists."""
        ...

    def get_collection(self, collection_name: str) -> models.CollectionInfo:
        """Read configuration and status for the named collection."""
        ...

    def count(self, collection_name: str, *, exact: bool) -> models.CountResult:
        """Count collection points with the requested accuracy."""
        ...

    def close(self) -> None:
        """Release the client's resources."""
        ...


def _require(document: JSONObject, name: str) -> JSONValue:
    """Return a required collection field or raise ValueError if absent."""
    if name not in document:
        raise ValueError(f"Qdrant collection information misses required field {name!r}")
    return document[name]


class QdrantCollectionAdapter:
    """Read exact live collection state without retrieving points or payloads."""

    def __init__(self, config: QdrantConfig, *, client: _QdrantReader | None = None) -> None:
        """Store configuration and an optional client without connecting."""
        self._config = config
        self._client = client
        self._closed = False

    def evidence(self) -> QdrantCollectionEvidence | None:
        """Read collection identity and exact count, or return None if absent."""
        client = self._connect()
        if not client.collection_exists(self._config.collection):
            return None
        document = json_object(parse_json(client.get_collection(self._config.collection).config.model_dump_json()))
        metadata = json_object(_require(document, "metadata"))
        params = json_object(_require(document, "params"))
        vectors = json_object(_require(params, "vectors"))
        dense = json_object(_require(vectors, "dense"))
        dense_dimension = json_integer(_require(dense, "size"))
        if dense_dimension <= 0:
            raise ValueError(f"Qdrant collection reports dense dimension {dense_dimension}")
        return QdrantCollectionEvidence(
            collection=self._config.collection,
            build_id=json_string(_require(metadata, "build_generation")),
            dense_dimension=dense_dimension,
            point_count=client.count(self._config.collection, exact=True).count,
        )

    def close(self) -> None:
        """Close the client once and prevent further connections."""
        if self._closed:
            return
        self._closed = True
        if self._client is not None:
            self._client.close()

    def _connect(self) -> _QdrantReader:
        """Return the existing client or connect lazily unless already closed."""
        if self._closed:
            raise ValueError("Qdrant collection adapter is closed")
        if self._client is None:
            self._client = create_qdrant_client(self._config, self._config.request_timeout)
        return self._client
