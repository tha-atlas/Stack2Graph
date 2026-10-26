"""Define dump-store operations independently of database driver types."""

from __future__ import annotations

from typing import Protocol

from sql.domain.models import EncodedTableBatch, ImportProgress, PublishedSnapshot, TableBatch, ValidationReport
from utils.resources import BatchLimits


class BatchEncoder[Batch](Protocol):
    def __call__(self, batch: TableBatch, limits: BatchLimits) -> Batch:
        """Encode a table batch within the supplied row and byte limits."""
        ...


class DumpStore(Protocol):
    @property
    def batch_encoder(self) -> BatchEncoder[EncodedTableBatch]:
        """Return the adapter's encoded-batch preparation function."""
        ...

    def progress(self, table: str) -> ImportProgress:
        """Read durable import progress for a table."""
        ...

    def commit_batch(self, batch: TableBatch) -> ImportProgress:
        """Commit a logical batch and return its durable progress."""
        ...

    def commit_encoded_batch(self, batch: EncodedTableBatch) -> ImportProgress:
        """Commit an encoded batch and return its durable progress."""
        ...

    def finish_table(self, table: str, xml_rows: int) -> ImportProgress:
        """Verify the XML row count and mark the table complete."""
        ...

    def validate(self) -> ValidationReport:
        """Validate the imported snapshot and return its evidence report."""
        ...

    def publish(self, validation: ValidationReport | None = None) -> PublishedSnapshot:
        """Publish the snapshot using the supplied or freshly checked report."""
        ...

    def close(self) -> None:
        """Release the store's database resources."""
        ...
