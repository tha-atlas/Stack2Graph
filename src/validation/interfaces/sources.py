"""Define count-evidence readers whose adapters manage external resources."""

from typing import Protocol

from validation.domain.models import (
    GraphArtifactEvidence,
    QdrantCollectionEvidence,
    SnapshotTarget,
    TableCountEvidence,
    TableCounts,
    VectorArtifactEvidence,
)


class PostgreSQLCountSource(Protocol):
    """Read counts from one published snapshot and manage its connection lifetime."""

    @property
    def snapshot_target(self) -> SnapshotTarget:
        """The snapshot identity pinned for all count queries."""
        ...

    def table_counts(self) -> TableCountEvidence:
        """Read XML parser counts and measured database counts per table."""
        ...

    def window_question_count(self, *, after_id: int, limit: int | None) -> int:
        """Count questions after the given ID within the optional row limit."""
        ...

    def close(self) -> None:
        """Release the snapshot reader's resources."""
        ...


class GraphArtifactSource(Protocol):
    def evidence(self) -> GraphArtifactEvidence:
        """Read graph provenance and processed source-row counts."""
        ...


class VectorArtifactSource(Protocol):
    def evidence(self) -> VectorArtifactEvidence:
        """Read vector provenance and finalized processing counts."""
        ...


class QdrantCollectionSource(Protocol):
    """Read exact live state, returning None when the collection is absent."""

    def evidence(self) -> QdrantCollectionEvidence | None:
        """Read collection identity and counts, or return None if absent."""
        ...

    def close(self) -> None:
        """Release the collection client's resources."""
        ...


class GraphExportCountSource(Protocol):
    """Count source rows accepted by the same filters used for graph export."""

    def export_table_counts(self) -> TableCounts:
        """Count source rows using the graph export's selection rules."""
        ...
