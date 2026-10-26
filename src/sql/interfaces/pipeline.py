"""Define source, download, extraction, and publication operations."""

from collections.abc import Mapping
from typing import Protocol

from sql.domain.models import ArchiveChecksum, BinaryEvidence, SnapshotIdentity, XMLInventory
from sql.interfaces.store import DumpStore
from utils.config import SnapshotPaths
from utils.resources import ResourcePlan
from utils.types import JSONObject
from validation.domain.models import TableCounts


class SnapshotSource(Protocol):
    @property
    def metadata(self) -> Mapping[str, JSONObject]:
        """The source metadata gathered while resolving the snapshot."""
        ...

    def resolve(self, requested_snapshot: str, /) -> SnapshotIdentity:
        """Resolve a requested snapshot selector to a pinned identity."""
        ...

    def verify_recorded(self, identity: SnapshotIdentity, /) -> None:
        """Verify that a recorded identity still matches the remote source."""
        ...


class Downloader(Protocol):
    def download(self, identity: SnapshotIdentity, paths: SnapshotPaths, /) -> ArchiveChecksum:
        """Download and verify the archive for the pinned snapshot."""
        ...


class Extractor(Protocol):
    def preflight(self) -> BinaryEvidence | None:
        """Check extractor availability and return binary evidence when available."""
        ...

    def extract(
        self,
        identity: SnapshotIdentity,
        paths: SnapshotPaths,
        resources: ResourcePlan,
        archive_checksum: ArchiveChecksum,
        /,
    ) -> XMLInventory:
        """Extract verified XML inputs within the assigned resource budget."""
        ...


class PublicationValidator(Protocol):
    """
    Validate snapshot counts before writing its durable publication marker.

    The caller supplies the snapshot identity, XML row counts from durable
    import progress, and freshly validated PostgreSQL counts. Any exception,
    normally `ValidationMismatchError`, blocks publication. Committed rows are
    preserved, and the publication marker remains unwritten.
    """

    def __call__(self, identity: SnapshotIdentity, xml_counts: TableCounts, postgres_counts: TableCounts) -> None:
        """Compare XML and database counts before allowing snapshot publication."""
        ...


class StoreFactory(Protocol):
    def preflight(self, paths: SnapshotPaths, resources: ResourcePlan, /) -> tuple[str, ...]:
        """Check database readiness and return runtime version evidence."""
        ...

    def reset(self, paths: SnapshotPaths, identity: SnapshotIdentity, *, overwrite: bool) -> None:
        """Reset matching snapshot output under the explicit overwrite policy."""
        ...

    def __call__(
        self,
        paths: SnapshotPaths,
        identity: SnapshotIdentity,
        inventory: XMLInventory,
        resources: ResourcePlan,
        /,
    ) -> DumpStore:
        """Open a store for the pinned identity and verified XML inventory."""
        ...
