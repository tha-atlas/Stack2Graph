"""Stream bounded SQL results from a fixed, published snapshot schema."""

from __future__ import annotations

import os
import time
from collections.abc import Generator, Iterator, Sequence
from contextlib import closing
from typing import Protocol, runtime_checkable

from psycopg import Connection, Cursor, OperationalError, sql
from psycopg.rows import RowFactory
from psycopg_pool import ConnectionPool

from sql.adapter.postgres.postgres_connection import connect, connection_info, row_factory
from sql.adapter.postgres.postgres_store import (
    CATALOG,
    configure_session,
    metadata_query,
    projection,
    schema_name,
    table_sql,
)
from sql.domain.models import Row, SnapshotIdentity, ValidationReport
from sql.domain.postgres import PostgreSQLConfig
from sql.domain.read_errors import SnapshotRecordBudgetError
from sql.domain.rows import integer, text
from sql.domain.schema import IMPORT_VERSION, SCHEMA_VERSION
from utils.backoff import retry_delay
from utils.config import DataPaths, SnapshotPaths
from utils.manifest import ManifestStore
from utils.resources import ResourcePlan
from utils.types import json_object, parse_json


@runtime_checkable
class _SQLFailure(Protocol):
    sqlstate: str | None


_OPERATIONAL_ERROR: type[Exception] = OperationalError


def stream_rows(
    cursor: Cursor[Row], query: sql.Composed, parameters: Sequence[int | list[str | int]]
) -> Generator[Row]:
    """Stream driver rows and close the stream when reading ends."""
    records = cursor.stream(query, parameters, size=1)
    if not isinstance(records, Generator):
        raise TypeError("PostgreSQL driver did not return a closeable row stream")
    with closing(records):
        yield from records


class PostgreSQLSnapshotReader:
    def __init__(
        self,
        paths: SnapshotPaths | DataPaths,
        resources: ResourcePlan,
        config: PostgreSQLConfig,
        *,
        snapshot_id: str | None = None,
        read_attempts: int = 3,
    ) -> None:
        """Pin a published snapshot and open a process-local connection pool."""
        if not 0 < read_attempts <= 10:
            raise ValueError("Question read attempts must be between 1 and 10")
        self.read_attempts = read_attempts
        self._read_retries = 0
        self.pid, self.resources, self.config = os.getpid(), resources, config
        self._closed = False
        with connect(config) as connection:
            entry: Row | None
            if snapshot_id is not None:
                entries = connection.execute(
                    sql.SQL("SELECT * FROM {} WHERE status=%s AND item_id=%s LIMIT 2").format(
                        table_sql(CATALOG, "snapshots")
                    ),
                    ("published", snapshot_id),
                ).fetchall()
                if len(entries) != 1:
                    raise ValueError("Snapshot selector must identify exactly one published PostgreSQL snapshot")
                entry = entries[0]
            elif isinstance(paths, DataPaths):
                workspace = str(paths.data_dir / "snapshots")
                entry = connection.execute(
                    sql.SQL(
                        "SELECT s.* FROM {} a JOIN {} s USING(schema_name) WHERE a.workspace=%s AND s.status=%s"
                    ).format(table_sql(CATALOG, "active"), table_sql(CATALOG, "snapshots")),
                    (workspace, "published"),
                ).fetchone()
            else:
                state = ManifestStore(paths.manifest).read()
                if state.get("backend") != "postgresql" or not state.get("identity"):
                    raise ValueError("Snapshot lacks PostgreSQL identity; prepare and publish it first")
                identity = SnapshotIdentity.from_dict(json_object(state["identity"]))
                expected_schema = schema_name(paths, identity)
                entry = connection.execute(
                    sql.SQL("SELECT * FROM {} WHERE schema_name=%s AND status=%s").format(
                        table_sql(CATALOG, "snapshots")
                    ),
                    (expected_schema, "published"),
                ).fetchone()
            if entry is None:
                raise ValueError("No complete published PostgreSQL snapshot")
            self.schema = text(entry, "schema_name")
            metadata = connection.execute(metadata_query(self.schema)).fetchall()
            values = {text(row, "key"): text(row, "value") for row in metadata}
            self._snapshot_identity = SnapshotIdentity.from_dict(json_object(parse_json(values["identity_json"])))
            self.validation = ValidationReport.from_dict(json_object(parse_json(text(entry, "validation"))))
            if (
                values.get("schema") != SCHEMA_VERSION
                or values.get("import") != IMPORT_VERSION
                or values["identity"] != self._snapshot_identity.digest
                or self.validation.identity != values["identity"]
                or self.validation.inventory != values["inventory"]
            ):
                raise ValueError("Published PostgreSQL metadata mismatch")
        options: dict[str, bool | RowFactory[Row]] = {"row_factory": row_factory, "autocommit": True}
        self.pool: ConnectionPool[Connection[Row]] = ConnectionPool(
            connection_info(config),
            connection_class=Connection[Row],
            kwargs=options,
            min_size=0,
            max_size=config.pool_size,
            max_waiting=config.pool_size * 4,
            timeout=config.connect_timeout,
            open=True,
            configure=self._configure,
            check=self._check_connection,
        )

    def _check_connection(self, connection: Connection[Row]) -> None:
        """Probe a pooled connection with a read-only statement."""
        connection.execute("SELECT 1")

    def _configure(self, connection: Connection[Row]) -> None:
        """Apply reader resource limits and default read-only transactions."""
        configure_session(connection, self.resources, readers=self.config.pool_size)
        connection.execute("SELECT set_config('default_transaction_read_only', 'on', false)")

    @property
    def snapshot_identity(self) -> SnapshotIdentity:
        """The identity of the published snapshot pinned by this reader."""
        return self._snapshot_identity

    def _check_process(self) -> None:
        """Reject closed readers and readers inherited from another process."""
        if self.pid != os.getpid() or self._closed:
            raise RuntimeError(
                "Open a separate snapshot reader after process start; closed/inherited readers cannot be used"
            )

    def check_process(self) -> None:
        """Confirm this reader belongs to the current process and is still open."""
        self._check_process()

    def _rows(
        self, query: sql.Composed, parameters: Sequence[int | list[str | int]], batch_rows: int
    ) -> Iterator[tuple[Row, ...]]:
        """Stream query results within configured record, batch, and byte limits."""
        self._check_process()
        if not 0 < batch_rows <= self.resources.batches.max_rows:
            raise ValueError("Requested result batch exceeds configured row budget")
        with (
            self.pool.connection() as connection,
            connection.transaction(),
            connection.cursor() as cursor,
            closing(stream_rows(cursor, query, parameters)) as records,
        ):
            rows: list[Row] = []
            size = 0
            # libpq single-row mode: bounded rows without a FETCH query per record.
            for row in records:
                row_bytes = 128 + sum(
                    len(str(value).encode("utf-8")) + 64 for value in row.values() if value is not None
                )
                if row_bytes > self.resources.batches.max_record_bytes:
                    raw_id = row.get("source_id", row.get("Id", row.get("event_id")))
                    source_id = raw_id if isinstance(raw_id, int) and not isinstance(raw_id, bool) else 0
                    raw_table = row.get("source_table")
                    table = raw_table if isinstance(raw_table, str) else "Snapshot"
                    _, field = max(
                        (len(str(value).encode("utf-8")), name) for name, value in row.items() if value is not None
                    )
                    raise SnapshotRecordBudgetError(table, source_id, field)
                if rows and (len(rows) >= batch_rows or size + row_bytes > self.resources.batches.max_bytes):
                    yield tuple(rows)
                    rows, size = [], 0
                rows.append(row)
                size += row_bytes
            if rows:
                yield tuple(rows)

    def iter_projected_rows(
        self, query: sql.Composed, parameters: Sequence[int | list[str | int]], batch_rows: int
    ) -> Iterator[tuple[Row, ...]]:
        """Stream a caller's read-only query within row and byte budgets."""
        yield from self._rows(query, parameters, batch_rows)

    def count_questions(self) -> int:
        """Count question posts in the pinned snapshot."""
        self._check_process()
        with self.pool.connection() as connection:
            row = connection.execute(
                sql.SQL("SELECT count(*) AS count FROM {} WHERE {}=1").format(
                    table_sql(self.schema, "Posts"), sql.Identifier("PostTypeId")
                )
            ).fetchone()
            return integer(row, "count")

    def iter_questions(self, batch_rows: int = 1024) -> Iterator[tuple[Row, ...]]:
        """
        Page through questions by composite key, retaining every selected
        field.
        """
        self._check_process()
        if not 0 < batch_rows <= self.resources.batches.max_rows:
            raise ValueError("Requested result batch exceeds configured row budget")
        last_id = -(1 << 63)
        first = True
        while True:
            predicate = sql.SQL("") if first else sql.SQL(" AND {} > %s").format(sql.Identifier("Id"))
            query = sql.SQL("SELECT {} FROM {} WHERE {}=%s{} ORDER BY {} LIMIT %s").format(
                projection("Posts"),
                table_sql(self.schema, "Posts"),
                sql.Identifier("PostTypeId"),
                predicate,
                sql.Identifier("Id"),
            )
            parameters: list[int | list[str | int]] = [1, batch_rows] if first else [1, last_id, batch_rows]
            received = 0
            for batch in self._rows(query, parameters, batch_rows):
                received += len(batch)
                last_id = integer(batch[-1], "Id")
                yield batch
            if received < batch_rows:
                return
            first = False

    @property
    def read_retries(self) -> int:
        """The number of source-read retries performed by this reader."""
        return self._read_retries

    def iter_question_rows(self, batch_rows: int = 1024, *, after_id: int = -(1 << 63)) -> Iterator[tuple[Row, ...]]:
        """Stream question text and metadata fields, without requiring local dump files."""
        self._check_process()
        if not 0 < batch_rows <= self.resources.batches.max_rows:
            raise ValueError("Requested result batch exceeds configured row budget")
        fields = (
            "Id",
            "Title",
            "Body",
            "Tags",
            "Score",
            "AnswerCount",
            "AcceptedAnswerId",
            "CreationDate",
            "LastEditDate",
            "LastActivityDate",
            "ClosedDate",
        )
        if not -(1 << 63) <= after_id < 1 << 63:
            raise ValueError("Keyset start must be a PostgreSQL bigint")
        last_id = after_id
        failures = 0
        expressions: list[sql.Composable] = [sql.Identifier(name) for name in fields]
        while True:
            query = sql.SQL("SELECT {} FROM {} WHERE {}=%s AND {}>%s ORDER BY {} LIMIT %s").format(
                sql.SQL(", ").join(expressions),
                table_sql(self.schema, "Posts"),
                sql.Identifier("PostTypeId"),
                sql.Identifier("Id"),
                sql.Identifier("Id"),
            )
            received = 0
            try:
                for batch in self._rows(query, [1, last_id, batch_rows], batch_rows):
                    received += len(batch)
                    last_id = integer(batch[-1], "Id")
                    yield batch
            except _OPERATIONAL_ERROR as error:
                if not isinstance(error, _SQLFailure):
                    raise
                failures += 1
                transient = (
                    error.sqlstate is None
                    or error.sqlstate.startswith("08")
                    or error.sqlstate in ("57P01", "57P02", "57P03")
                )
                if not transient or failures >= self.read_attempts:
                    raise
                self._read_retries += 1
                time.sleep(retry_delay(failures - 1))
                # Only the in-process last yielded ID is retained; there is no checkpoint.
                continue
            failures = 0
            if received < batch_rows:
                return

    def close(self) -> None:
        """Close the connection pool once and prevent further reads."""
        if not self._closed:
            self.pool.close()
            self._closed = True
