"""Define count evidence and exceptions that retain failed check results."""

from dataclasses import dataclass
from typing import Literal

from utils.config import validate_item_id
from utils.types import JSONObject
from validation.domain.results import CheckResults

IMPORTED_TABLES: tuple[str, ...] = ("Posts", "Comments", "Tags", "Votes", "PostLinks")
TableCounts = tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class ValidationRequest:
    """Select optional snapshot, graph, vector, and validation run names."""

    run: str | None = None
    snapshot: str | None = None
    graph_run: str | None = None
    vector_run: str | None = None

    def __post_init__(self) -> None:
        """Validate every supplied run or snapshot identifier."""
        for name in (self.run, self.snapshot, self.graph_run, self.vector_run):
            if name is not None:
                validate_item_id(name)


@dataclass(frozen=True)
class SnapshotTarget:
    """Identify the fixed PostgreSQL snapshot used for count comparisons."""

    snapshot_id: str
    snapshot_digest: str

    def to_json(self) -> JSONObject:
        """Return the snapshot ID and digest as JSON fields."""
        return {"snapshot_id": self.snapshot_id, "snapshot_digest": self.snapshot_digest}


@dataclass(frozen=True)
class VectorArtifactEvidence:
    """Describe a finalized vector run using metadata rather than shard contents."""

    run: str
    source_snapshot_id: str
    source_snapshot_digest: str
    registry_digest: str
    collection: str
    build_id: str
    dense_dimension: int
    scanned_questions: int
    finalized_points: int
    selected_questions: int
    after_id: int
    question_limit: int | None

    def binding(self) -> JSONObject:
        """Return identity and settings that bind evidence to this artifact."""
        return {
            "source_snapshot_id": self.source_snapshot_id,
            "source_snapshot_digest": self.source_snapshot_digest,
            "registry_digest": self.registry_digest,
            "collection": self.collection,
            "build_id": self.build_id,
            "dense_dimension": self.dense_dimension,
            "after_id": self.after_id,
            "question_limit": self.question_limit,
        }

    def to_json(self) -> JSONObject:
        """Serialize vector identity, source window, and processing counts."""
        return {
            "run": self.run,
            "source_snapshot_id": self.source_snapshot_id,
            "source_snapshot_digest": self.source_snapshot_digest,
            "registry_digest": self.registry_digest,
            "collection": self.collection,
            "build_id": self.build_id,
            "dense_dimension": self.dense_dimension,
            "scanned_questions": self.scanned_questions,
            "finalized_points": self.finalized_points,
            "selected_questions": self.selected_questions,
            "after_id": self.after_id,
            "question_limit": self.question_limit,
        }


@dataclass(frozen=True)
class GraphArtifactEvidence:
    """Record graph manifest metadata and independently processed source counts."""

    run: str
    source_snapshot_id: str
    source_snapshot_digest: str
    registry_digest: str
    exporter_digest: str
    exported_questions: int
    processed_table_counts: TableCounts

    def binding(self) -> JSONObject:
        """Return the source, registry, and exporter identity fields."""
        return {
            "source_snapshot_id": self.source_snapshot_id,
            "source_snapshot_digest": self.source_snapshot_digest,
            "registry_digest": self.registry_digest,
            "exporter_digest": self.exporter_digest,
        }

    def to_json(self) -> JSONObject:
        """Serialize graph identity and processed source-table counts."""
        return {
            "run": self.run,
            "source_snapshot_id": self.source_snapshot_id,
            "source_snapshot_digest": self.source_snapshot_digest,
            "registry_digest": self.registry_digest,
            "exporter_digest": self.exporter_digest,
            "exported_questions": self.exported_questions,
            "processed_table_counts": [[table, count] for table, count in self.processed_table_counts],
        }


@dataclass(frozen=True)
class QdrantCollectionEvidence:
    """Record exact collection state read live for the current invocation."""

    collection: str
    build_id: str
    dense_dimension: int
    point_count: int

    def to_json(self) -> JSONObject:
        """Serialize collection identity, dense dimension, and point count."""
        return {
            "collection": self.collection,
            "build_id": self.build_id,
            "dense_dimension": self.dense_dimension,
            "point_count": self.point_count,
        }


@dataclass(frozen=True)
class TableCountEvidence:
    """Keep XML counts, measured database counts, and their source evidence."""

    xml_counts: TableCounts
    measured_counts: TableCounts
    source: Literal["published-evidence", "fresh-scan"]

    def to_json(self) -> JSONObject:
        """Serialize XML and database counts with their evidence source."""
        return {
            "xml_counts": [[table, count] for table, count in self.xml_counts],
            "measured_counts": [[table, count] for table, count in self.measured_counts],
            "source": self.source,
        }


class ValidationMismatchError(ValueError):
    """Retain all comparison results from a failed validation check."""

    def __init__(self, stage: str, results: CheckResults) -> None:
        """Retain failed check results and describe the failing stage."""
        self.stage = stage
        self.results = results
        failed = ", ".join(f"{item.check} ({item.reason})" for item in results.failures)
        super().__init__(f"Validation failed in {stage}: {failed}")
