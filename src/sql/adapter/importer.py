"""Load PostgreSQL adapters only for commands that need the driver."""

from sql.domain.models import SnapshotIdentity, XMLInventory
from sql.domain.postgres import PostgreSQLConfig
from sql.interfaces.store import DumpStore
from utils.config import SnapshotPaths
from utils.resources import ResourcePlan


class PostgreSQLStoreFactory:
    def __init__(self, config: PostgreSQLConfig) -> None:
        """Store the PostgreSQL connection configuration."""
        self.config = config

    def preflight(self, _paths: SnapshotPaths, resources: ResourcePlan) -> tuple[str, ...]:
        """Check connectivity, configure session limits, and read runtime versions."""
        from sql.adapter.postgres.postgres_connection import connect, runtime_identity
        from sql.adapter.postgres.postgres_store import configure_session

        with connect(self.config) as connection:
            configure_session(connection, resources)
            return runtime_identity(connection)

    def __call__(
        self,
        paths: SnapshotPaths,
        identity: SnapshotIdentity,
        inventory: XMLInventory,
        resources: ResourcePlan,
    ) -> DumpStore:
        """Open a dump store for the supplied snapshot and XML inventory."""
        from sql.adapter.postgres.postgres_store import PostgreSQLDumpStore

        return PostgreSQLDumpStore(paths, identity, inventory, resources, self.config)

    def reset(self, paths: SnapshotPaths, identity: SnapshotIdentity, *, overwrite: bool) -> None:
        """Reset the snapshot database when overwrite is explicitly allowed."""
        from sql.adapter.postgres.postgres_store import reset_snapshot

        reset_snapshot(paths, identity, self.config, overwrite=overwrite)
