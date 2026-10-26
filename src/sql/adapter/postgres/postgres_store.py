"""Write COPY batches with locked progress and publish validated snapshots."""

from __future__ import annotations

import json
from collections.abc import Callable
from hashlib import sha256
from time import sleep

from psycopg import Connection, sql

from sql.adapter.postgres.dump_batches import encode_batch
from sql.adapter.postgres.postgres_connection import connect, runtime_identity
from sql.domain.models import (
    EncodedTableBatch,
    ImportProgress,
    PublishedSnapshot,
    Row,
    SnapshotIdentity,
    TableBatch,
    ValidationReport,
    XMLInventory,
)
from sql.domain.postgres import PostgreSQLConfig
from sql.domain.rows import integer, text
from sql.domain.schema import IMPORT_VERSION, SCHEMA_VERSION, TABLES
from sql.interfaces.store import BatchEncoder
from utils.config import SnapshotPaths
from utils.logging_utils import get_logger, stage_event
from utils.manifest import ManifestStore
from utils.resources import ResourcePlan, require_disk, require_memory
from utils.types import JSONObject, json_object, parse_json

LOGGER = get_logger(__name__)
CATALOG = "stack2graph_catalog"
LOOKUP_INDEXES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("posts_type_id", "Posts", ("PostTypeId", "Id")),
    ("posts_parent_type", "Posts", ("ParentId", "PostTypeId")),
    ("comments_post", "Comments", ("PostId",)),
    ("tags_name", "Tags", ("TagName",)),
    ("votes_post_type", "Votes", ("PostId", "VoteTypeId")),
    ("links_post", "PostLinks", ("PostId",)),
    ("links_related", "PostLinks", ("RelatedPostId",)),
)
COLUMN_TYPES: dict[str, sql.SQL] = {
    "BIGINT": sql.SQL("bigint"),
    "BOOLEAN": sql.SQL("boolean"),
    "VARCHAR": sql.SQL("text"),
    "TIMESTAMP_TEXT": sql.SQL("text"),
    "JSON": sql.SQL("jsonb"),
}


def schema_name(paths: SnapshotPaths, identity: SnapshotIdentity) -> str:
    """Derive a snapshot schema name from its state path and identity."""
    return "snapshot_" + sha256((str(paths.state) + "\n" + identity.digest).encode()).hexdigest()[:40]


def table_sql(schema: str, table: str) -> sql.Identifier:
    """Quote a schema-qualified table identifier."""
    return sql.Identifier(schema, table)


def metadata_query(schema: str) -> sql.Composed:
    """Build the snapshot metadata SELECT shared by the store and reader."""
    return sql.SQL("SELECT key, value FROM {}").format(table_sql(schema, "_metadata"))


def reset_snapshot(
    paths: SnapshotPaths, identity: SnapshotIdentity, config: PostgreSQLConfig, *, overwrite: bool
) -> None:
    """Drop matching snapshot schemas, requiring overwrite for existing output."""
    workspace = paths.workspace
    with connect(config) as connection:
        catalog = connection.execute("SELECT to_regclass(%s) AS name", (f"{CATALOG}.snapshots",)).fetchone()
        if catalog is None or catalog["name"] is None:
            return
        entries = connection.execute(
            sql.SQL("SELECT schema_name FROM {} WHERE workspace=%s AND item_id=%s").format(
                table_sql(CATALOG, "snapshots")
            ),
            (workspace, identity.item_id),
        ).fetchall()
        if entries and not overwrite:
            raise ValueError("PostgreSQL output already exists; pass --overwrite to rebuild from the XML inputs")
        for entry in entries:
            schema = text(entry, "schema_name")
            lock_key = int(sha256(schema.encode()).hexdigest()[:15], 16)
            lock = connection.execute("SELECT pg_try_advisory_lock(%s) AS locked", (lock_key,)).fetchone()
            if lock is None or lock["locked"] is not True:
                raise RuntimeError("Snapshot has another coordinated PostgreSQL writer")
            with connection.transaction():
                connection.execute(
                    sql.SQL("DELETE FROM {} WHERE schema_name=%s").format(table_sql(CATALOG, "active")),
                    (schema,),
                )
                connection.execute(
                    sql.SQL("DELETE FROM {} WHERE schema_name=%s").format(table_sql(CATALOG, "snapshots")),
                    (schema,),
                )
                connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


def projection(table: str, alias: str = "") -> sql.Composed:
    """Build a logical table projection, exposing JSON columns as text."""
    prefix = (alias,) if alias else ()
    expressions: list[sql.Composable] = []
    for column in TABLES[table].columns:
        value: sql.Composable = sql.Identifier(*prefix, column.name)
        if column.kind == "JSON":
            value = sql.SQL("{}::text AS {}").format(value, sql.Identifier(column.name))
        expressions.append(value)
    return sql.SQL(", ").join(expressions)


def configure_session(connection: Connection[Row], resources: ResourcePlan, *, readers: int = 1) -> None:
    """Honor server limits and bound multiplicative session/operator memory."""
    limits = connection.execute(
        "SELECT pg_size_bytes(current_setting('work_mem')) AS work, "
        "pg_size_bytes(current_setting('maintenance_work_mem')) AS maintenance, "
        "current_setting('max_connections')::bigint AS connections, "
        "CASE WHEN current_setting('temp_file_limit')='-1' THEN -1 ELSE "
        "pg_size_bytes(current_setting('temp_file_limit')) END AS spill"
    ).fetchone()
    work = max(
        64 * 1024,
        min(integer(limits, "work"), resources.database_bytes // (max(readers, integer(limits, "connections")) * 32)),
    )
    maintenance = max(1024**2, min(integer(limits, "maintenance"), resources.database_bytes // 8))
    server_spill = integer(limits, "spill")
    spill = min(server_spill, resources.spill_bytes) if server_spill >= 0 else resources.spill_bytes
    for key, value in (
        ("work_mem", f"{work // 1024}kB"),
        ("maintenance_work_mem", f"{maintenance // 1024}kB"),
        ("temp_file_limit", f"{spill // 1024}kB"),
        ("max_parallel_workers_per_gather", "0"),
        ("synchronous_commit", "on"),
    ):
        connection.execute("SELECT set_config(%s, %s, false)", (key, value))


class PostgreSQLDumpStore:
    def __init__(
        self,
        paths: SnapshotPaths,
        identity: SnapshotIdentity,
        inventory: XMLInventory,
        resources: ResourcePlan,
        config: PostgreSQLConfig,
    ) -> None:
        """Open and lock a snapshot store, initializing or checking its metadata."""
        self.paths, self.identity, self.inventory = paths, identity, inventory
        self.resources, self.config = resources, config
        self.schema = schema_name(paths, identity)
        self._connection: Connection[Row] | None = None
        self._closed = False
        self._published = False
        self.lock_key = int(sha256(self.schema.encode()).hexdigest()[:15], 16)
        if (
            identity.item_id != paths.item_id
            or len(inventory.inputs) != len(TABLES)
            or {x.table for x in inventory.inputs} != set(TABLES)
        ):
            raise ValueError("Snapshot identity or selected inventory mismatch")
        for source in inventory.inputs:
            if (
                not source.path.is_relative_to(paths.xml)
                or source.path.is_symlink()
                or not source.path.is_file()
                or source.path.stat().st_size != source.size
            ):
                raise ValueError(f"Input path/size mismatch: {source.table}")
        input_sizes = {x.table: x.size for x in inventory.inputs}
        self.expected_metadata = {
            "identity": identity.digest,
            "identity_json": json.dumps(identity.to_json(), sort_keys=True),
            "inventory": inventory.digest,
            "schema": SCHEMA_VERSION,
            "import": IMPORT_VERSION,
            "input_sizes": json.dumps(input_sizes, sort_keys=True),
        }
        try:
            self._open()
            self._initialize()
            self._check_metadata()
        except BaseException:
            self.close()
            raise

    @property
    def connection(self) -> Connection[Row]:
        """The open store connection; access after closing raises RuntimeError."""
        if self._connection is None or self._closed:
            raise RuntimeError("The PostgreSQL dump store is closed")
        return self._connection

    def _open(self) -> None:
        """Connect, apply resource limits, and acquire the snapshot writer lock."""
        self._connection = connect(self.config)
        self.runtime = runtime_identity(self.connection)
        configure_session(self.connection, self.resources)
        locked = self.connection.execute("SELECT pg_try_advisory_lock(%s) AS locked", (self.lock_key,)).fetchone()
        if locked is None or locked["locked"] is not True:
            raise ValueError("Snapshot has another coordinated PostgreSQL writer")

    def _initialize(self) -> None:
        """Create catalog and snapshot tables within a setup transaction."""
        with self.connection.transaction():
            # Serializes only short catalogue setup, not COPY or reader lifetimes.
            self.connection.execute("SELECT pg_advisory_xact_lock(%s)", (3482758390471,))
            self.connection.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(CATALOG)))
            self.connection.execute(
                sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {} (schema_name text PRIMARY KEY, workspace text NOT NULL, "
                    "item_id text NOT NULL, identity text NOT NULL, inventory text NOT NULL, status text NOT NULL, "
                    "validation text, UNIQUE(workspace, item_id))"
                ).format(table_sql(CATALOG, "snapshots"))
            )
            self.connection.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {} (workspace text PRIMARY KEY, schema_name text NOT NULL)").format(
                    table_sql(CATALOG, "active")
                )
            )
            entry = self.connection.execute(
                sql.SQL("SELECT * FROM {} WHERE workspace=%s AND item_id=%s").format(table_sql(CATALOG, "snapshots")),
                (self.paths.workspace, self.paths.item_id),
            ).fetchone()
            if entry is not None:
                if (
                    text(entry, "schema_name") != self.schema
                    or text(entry, "identity") != self.identity.digest
                    or text(entry, "inventory") != self.inventory.digest
                ):
                    raise ValueError("PostgreSQL snapshot identity/inventory conflict")
                self._published = text(entry, "status") == "published"
                return
            exists = self.connection.execute(
                "SELECT count(*) AS count FROM pg_namespace WHERE nspname=%s", (self.schema,)
            ).fetchone()
            if integer(exists, "count"):
                raise ValueError("Unlabelled PostgreSQL schema; do not adopt existing data")
            self.connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
            for spec in TABLES.values():
                definitions: list[sql.Composable] = [
                    sql.SQL("{} {} {}").format(
                        sql.Identifier(c.name),
                        COLUMN_TYPES[c.kind],
                        sql.SQL("" if c.nullable else "NOT NULL"),
                    )
                    for c in spec.columns
                ]
                columns = sql.SQL(", ").join(definitions)
                self.connection.execute(
                    sql.SQL("CREATE TABLE {} ({})").format(table_sql(self.schema, spec.name), columns)
                )
            self.connection.execute(
                sql.SQL("CREATE TABLE {} (key text PRIMARY KEY, value text NOT NULL)").format(
                    table_sql(self.schema, "_metadata")
                )
            )
            self.connection.execute(
                sql.SQL(
                    "CREATE TABLE {} (table_name text PRIMARY KEY, position bigint NOT NULL, row_count bigint NOT NULL, eof boolean NOT NULL)"
                ).format(table_sql(self.schema, "_progress"))
            )
            self.connection.execute(
                sql.SQL(
                    "CREATE TABLE {} (table_name text NOT NULL, first_position bigint NOT NULL, row_count bigint NOT NULL, digest text NOT NULL, PRIMARY KEY(table_name, first_position))"
                ).format(table_sql(self.schema, "_batches"))
            )
            for key, value in self.expected_metadata.items():
                self.connection.execute(
                    sql.SQL("INSERT INTO {} VALUES (%s, %s)").format(table_sql(self.schema, "_metadata")), (key, value)
                )
            for table in TABLES:
                self.connection.execute(
                    sql.SQL("INSERT INTO {} VALUES (%s, 0, 0, false)").format(table_sql(self.schema, "_progress")),
                    (table,),
                )
            self.connection.execute(
                sql.SQL("INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, NULL)").format(table_sql(CATALOG, "snapshots")),
                (
                    self.schema,
                    self.paths.workspace,
                    self.paths.item_id,
                    self.identity.digest,
                    self.inventory.digest,
                    "preparing",
                ),
            )

    def _check_metadata(self) -> None:
        """Verify snapshot metadata and progress tables and read publication state."""
        rows = self.connection.execute(metadata_query(self.schema)).fetchall()
        values = {text(row, "key"): text(row, "value") for row in rows}
        for key, expected in self.expected_metadata.items():
            if values.get(key) != expected:
                raise ValueError(f"PostgreSQL snapshot/schema mismatch: {key}")
        entry = self.connection.execute(
            sql.SQL("SELECT status FROM {} WHERE schema_name=%s").format(table_sql(CATALOG, "snapshots")),
            (self.schema,),
        ).fetchone()
        self._published = text(entry, "status") == "published"
        names = self.connection.execute(
            sql.SQL("SELECT table_name FROM {}").format(table_sql(self.schema, "_progress"))
        ).fetchall()
        if {text(row, "table_name") for row in names} != set(TABLES):
            raise ValueError("PostgreSQL progress inventory mismatch")

    def _retry[Result](self, operation: Callable[[], Result]) -> Result:
        """Reconnect after connection loss and retry within the configured limit."""
        for attempt in range(self.config.retry_attempts):
            try:
                if self._connection is None:
                    self._open()
                    self._check_metadata()
                return operation()
            except Exception:
                if self._connection is not None and not self._connection.broken and not self._connection.closed:
                    raise
                if self._connection is not None:
                    self._connection.close()
                self._connection = None
                if attempt + 1 == self.config.retry_attempts:
                    raise RuntimeError("PostgreSQL connection lost; rerun import or use run --overwrite") from None
                LOGGER.warning("PostgreSQL connection lost; reconnecting and checking committed batch receipt")
                sleep(min(2.0, 0.1 * (1 << attempt)))
        raise RuntimeError("PostgreSQL retry budget exhausted")

    def progress(self, table: str) -> ImportProgress:
        """Read and validate durable import progress for a supported table."""
        if table not in TABLES:
            raise ValueError(f"Unsupported dump table: {table}")

        def read() -> ImportProgress:
            """Read committed row position, count, and end-of-file state."""
            row = self.connection.execute(
                sql.SQL("SELECT position, row_count, eof FROM {} WHERE table_name=%s").format(
                    table_sql(self.schema, "_progress")
                ),
                (table,),
            ).fetchone()
            position, count = integer(row, "position"), integer(row, "row_count")
            if row is None or not isinstance(row["eof"], bool) or position < 0 or position != count:
                raise ValueError(f"Invalid durable progress: {table}")
            return ImportProgress(table, position, count, row["eof"])

        return self._retry(read)

    @property
    def batch_encoder(self) -> BatchEncoder[EncodedTableBatch]:
        """Return the COPY batch encoder used by this store."""
        return encode_batch

    def commit_batch(self, batch: TableBatch) -> ImportProgress:
        """Encode and durably commit a logical table batch."""
        return self.commit_encoded_batch(encode_batch(batch, self.resources.batches))

    def commit_encoded_batch(self, batch: EncodedTableBatch) -> ImportProgress:
        """Commit bounded COPY data with replay receipts and durable progress."""
        limits = self.resources.batches
        if (
            batch.table not in TABLES
            or batch.row_count <= 0
            or batch.row_count > limits.max_rows
            or len(batch.payload) > limits.max_bytes * 2
            or batch.payload.count(b"\n") != batch.row_count
        ):
            raise ValueError("Encoded COPY batch row count or byte budget mismatch")
        if self._published:
            raise ValueError("Cannot modify a published snapshot")
        require_memory(limits.max_bytes * 4 + 64 * 1024**2)
        require_disk(self.paths.database_dir, len(batch.payload) * 4, self.resources.disk_reserve)
        digest = sha256(batch.payload).hexdigest()

        def commit() -> ImportProgress:
            """Commit new COPY rows or verify a matching durable batch receipt."""
            with self.connection.transaction():
                current = self.connection.execute(
                    sql.SQL("SELECT position, row_count, eof FROM {} WHERE table_name=%s FOR UPDATE").format(
                        table_sql(self.schema, "_progress")
                    ),
                    (batch.table,),
                ).fetchone()
                receipt = self.connection.execute(
                    sql.SQL("SELECT row_count, digest FROM {} WHERE table_name=%s AND first_position=%s").format(
                        table_sql(self.schema, "_batches")
                    ),
                    (batch.table, batch.first_position),
                ).fetchone()
                if receipt is not None:
                    if integer(receipt, "row_count") != batch.row_count or text(receipt, "digest") != digest:
                        raise ValueError("Conflicting COPY batch replay")
                else:
                    if (
                        current is None
                        or current["eof"] is True
                        or batch.first_position != integer(current, "position") + 1
                    ):
                        raise ValueError(f"Nonsequential or finished batch: {batch.table}")
                    with (
                        self.connection.cursor() as cursor,
                        cursor.copy(sql.SQL("COPY {} FROM STDIN").format(table_sql(self.schema, batch.table))) as copy,
                    ):
                        # Escape encoding is already bounded; send fixed-size blocks to libpq.
                        for offset in range(0, len(batch.payload), 1024**2):
                            copy.write(batch.payload[offset : offset + 1024**2])
                    self.connection.execute(
                        sql.SQL(
                            "UPDATE {} SET position=position+%s, row_count=row_count+%s WHERE table_name=%s"
                        ).format(table_sql(self.schema, "_progress")),
                        (batch.row_count, batch.row_count, batch.table),
                    )
                    self.connection.execute(
                        sql.SQL("INSERT INTO {} VALUES (%s, %s, %s, %s)").format(table_sql(self.schema, "_batches")),
                        (batch.table, batch.first_position, batch.row_count, digest),
                    )
            return self.progress(batch.table)

        return self._retry(commit)

    def finish_table(self, table: str, xml_rows: int) -> ImportProgress:
        """Mark XML EOF only when its row count matches durable progress."""

        def finish() -> ImportProgress:
            """Check the final XML count and transactionally set the EOF flag."""
            with self.connection.transaction():
                progress = self.progress(table)
                if progress.position != xml_rows:
                    raise ValueError(f"XML EOF/count mismatch: {table}")
                if not progress.eof:
                    if self._published:
                        raise ValueError("Cannot modify a published snapshot")
                    self.connection.execute(
                        sql.SQL("UPDATE {} SET eof=true WHERE table_name=%s").format(
                            table_sql(self.schema, "_progress")
                        ),
                        (table,),
                    )
            return self.progress(table)

        return self._retry(finish)

    def _check_schema(self, table: str) -> None:
        """Require the table's columns, types, and nullability to match the schema."""
        rows = self.connection.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (self.schema, table),
        ).fetchall()
        actual = tuple(
            (text(row, "column_name"), text(row, "data_type"), text(row, "is_nullable") == "YES") for row in rows
        )
        expected = tuple(
            (
                c.name,
                {
                    "BIGINT": "bigint",
                    "BOOLEAN": "boolean",
                    "VARCHAR": "text",
                    "TIMESTAMP_TEXT": "text",
                    "JSON": "jsonb",
                }[c.kind],
                c.nullable,
            )
            for c in TABLES[table].columns
        )
        if actual != expected:
            raise ValueError(f"PostgreSQL schema mismatch: {table}")

    def validate(self) -> ValidationReport:
        """Check metadata, XML files, table schemas, and exact database counts."""
        self._check_metadata()
        tables = self.connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=%s", (self.schema,)
        ).fetchall()
        if {text(row, "table_name") for row in tables} != set(TABLES) | {"_metadata", "_progress", "_batches"}:
            raise ValueError("PostgreSQL table inventory mismatch")
        counts: list[tuple[str, int]] = []
        xml_counts: list[tuple[str, int]] = []
        for source in self.inventory.inputs:
            if not source.path.is_file() or source.path.stat().st_size != source.size:
                raise ValueError(f"XML size changed: {source.table}")
            self._check_schema(source.table)
            progress = self.progress(source.table)
            if not progress.eof:
                raise ValueError(f"Missing successful XML EOF: {source.table}")
            row = self.connection.execute(
                sql.SQL("SELECT count(*) AS count, count({}) AS nonnull, count(DISTINCT {}) AS unique FROM {}").format(
                    sql.Identifier("Id"), sql.Identifier("Id"), table_sql(self.schema, source.table)
                )
            ).fetchone()
            count = integer(row, "count")
            # NULL-ID and uniqueness integrity stays a creation-stage error. The XML
            # counter versus the actual COUNT(*) is the public `xml-postgres` check;
            # validate only records both sides so the gate can decide, never pre-filter.
            if count != integer(row, "nonnull") or count != integer(row, "unique"):
                raise ValueError(f"NULL ID or uniqueness mismatch: {source.table}")
            counts.append((source.table, count))
            xml_counts.append((source.table, progress.position))
        return ValidationReport(
            self.identity.digest,
            self.inventory.digest,
            tuple(sorted(counts)),
            self.runtime,
            tuple(sorted(xml_counts)),
        )

    def _index_headroom(self, table: str) -> None:
        """Require free space for index work, spill files, and the disk reserve."""
        row = self.connection.execute(
            "SELECT pg_total_relation_size(%s::regclass) AS bytes",
            (table_sql(self.schema, table).as_string(self.connection),),
        ).fetchone()
        # Index pages, sort runs and WAL can coexist. This is a conservative preflight,
        # not a hard database filesystem quota; PostgreSQL also rejects ENOSPC.
        require_disk(
            self.paths.database_dir, integer(row, "bytes") * 2 + self.resources.spill_bytes, self.resources.disk_reserve
        )

    def _prepare_indexes(self) -> None:
        """Create lookup and unique indexes, then vacuum and analyze each table."""
        for table in TABLES:
            self._index_headroom(table)
            self.connection.execute(
                sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})").format(
                    sql.Identifier(table + "_id"), table_sql(self.schema, table), sql.Identifier("Id")
                )
            )
        for name, table, columns in LOOKUP_INDEXES:
            self._index_headroom(table)
            identifiers: list[sql.Composable] = [sql.Identifier(column) for column in columns]
            self.connection.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                    sql.Identifier(name), table_sql(self.schema, table), sql.SQL(", ").join(identifiers)
                )
            )
        for table in TABLES:
            self._index_headroom(table)
            self.connection.execute(sql.SQL("VACUUM (ANALYZE) {}").format(table_sql(self.schema, table)))

    def publish(self, validation: ValidationReport | None = None) -> PublishedSnapshot:
        """Publish validated database state and local snapshot metadata."""

        def publish_database() -> ValidationReport:
            """Build indexes and commit publication, or verify an existing publication."""
            report = self.validate() if validation is None else validation
            if validation is None:
                # Ungated fallback: without the public gate in front, the store itself
                # must refuse to publish while its ledger and the database diverge.
                expected = dict(report.xml_counts)
                mismatched = sorted(table for table, count in report.counts if expected.get(table) != count)
                if mismatched:
                    raise ValueError(f"XML versus PostgreSQL count mismatch: {', '.join(mismatched)}")
            if not self._published:
                self._prepare_indexes()
                encoded = json.dumps(report.to_json(), sort_keys=True)
                with self.connection.transaction():
                    self.connection.execute(
                        sql.SQL("UPDATE {} SET status=%s, validation=%s WHERE schema_name=%s").format(
                            table_sql(CATALOG, "snapshots")
                        ),
                        ("published", encoded, self.schema),
                    )
                    self.connection.execute(
                        sql.SQL(
                            "INSERT INTO {} VALUES (%s, %s) ON CONFLICT(workspace) DO UPDATE SET schema_name=excluded.schema_name"
                        ).format(table_sql(CATALOG, "active")),
                        (self.paths.workspace, self.schema),
                    )
            else:
                row = self.connection.execute(
                    sql.SQL("SELECT validation FROM {} WHERE schema_name=%s").format(table_sql(CATALOG, "snapshots")),
                    (self.schema,),
                ).fetchone()
                saved = ValidationReport.from_dict(json_object(parse_json(text(row, "validation"))))
                if (
                    saved.identity != report.identity
                    or saved.inventory != report.inventory
                    or saved.counts != report.counts
                ):
                    raise ValueError("Durable publication validation mismatch")
                report = saved
            self._published = True
            return report

        report = self._retry(publish_database)
        result = PublishedSnapshot(self.schema, self.identity, report)
        publication: JSONObject = {
            "backend": "postgresql",
            "schema": self.schema,
            "identity": self.identity.digest,
            "inventory": self.inventory.digest,
        }
        ManifestStore(self.paths.publication).write(publication)
        manifest = ManifestStore(self.paths.manifest)
        state = manifest.read()
        if state.get("identity") and state["identity"] != self.identity.to_json():
            raise ValueError("Publication manifest identity conflict")
        state.update(
            identity=self.identity.to_json(),
            backend="postgresql",
            stage="published",
            validation=report.to_json(),
            publication=publication,
            progress={table: {"position": count, "count": count, "eof": True} for table, count in report.counts},
        )
        manifest.write(state)
        LOGGER.info("Published validated PostgreSQL snapshot %s", self.identity.item_id)
        stage_event("snapshot_published", snapshot=self.identity.item_id, rows=dict(report.counts))
        return result

    def close(self) -> None:
        """Close the connection and mark the dump store closed."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        self._closed = True
