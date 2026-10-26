"""Define collection operations needed to upload vector points."""

from collections.abc import Sequence
from typing import Protocol

from vector.domain.models import CollectionSchema, CollectionState, VectorPoint


class PointStore(Protocol):
    def inspect(self) -> CollectionState | None:
        """Read collection state, or return None if the collection is absent."""
        ...

    def replace(self, expected: CollectionState | None, schema: CollectionSchema) -> None:
        """Replace the collection after checking its expected state."""
        ...

    def upload(self, points: Sequence[VectorPoint]) -> None:
        """Store a batch of vector points."""
        ...
