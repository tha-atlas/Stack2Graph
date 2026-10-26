"""Read counts for a fixed snapshot, reusing durable evidence when possible."""

from psycopg import sql

from graph.adapter.queries import RAW_QUESTION_COUNT, query
from sql.adapter.reader import PostgreSQLSnapshotReader
from sql.domain.rows import integer
from validation.domain.models import IMPORTED_TABLES, SnapshotTarget, TableCountEvidence, TableCounts


class PostgreSQLCountAdapter:
    """Manage one snapshot reader and expose counts without streaming rows."""

    def __init__(self, reader: PostgreSQLSnapshotReader) -> None:
        """Store the snapshot reader and initialize the window-count cache."""
        self._reader = reader
        self._window_counts: dict[tuple[int, int | None], int] = {}

    @property
    def snapshot_target(self) -> SnapshotTarget:
        """The snapshot ID and digest pinned by the reader."""
        identity = self._reader.snapshot_identity
        return SnapshotTarget(snapshot_id=identity.item_id, snapshot_digest=identity.digest)

    def table_counts(self) -> TableCountEvidence:
        """
        Pair parser progress counters with exact database counts.

        A published snapshot already carries its exact publish-time COUNT(*) per
        table in its validation report; that durable evidence is reused when it
        covers every imported table. Otherwise each table is counted once now.
        """
        xml_counts = self._xml_table_counts()
        published = dict(self._reader.validation.counts)
        missing = [table for table in IMPORTED_TABLES if table not in published]
        if missing:
            return TableCountEvidence(
                xml_counts=xml_counts,
                measured_counts=self._measured_table_counts(),
                source="fresh-scan",
            )
        return TableCountEvidence(
            xml_counts=xml_counts,
            measured_counts=tuple((table, published[table]) for table in IMPORTED_TABLES),
            source="published-evidence",
        )

    def window_question_count(self, *, after_id: int, limit: int | None) -> int:
        """Count questions in the vector run's configured source window."""
        key = (after_id, limit)
        cached = self._window_counts.get(key)
        if cached is not None:
            return cached
        statement = query(self._reader.schema, RAW_QUESTION_COUNT)
        with self._reader.pool.connection() as connection:
            count = integer(connection.execute(statement, (after_id,)).fetchone(), "count")
        result = count if limit is None else min(count, limit)
        self._window_counts[key] = result
        return result

    def close(self) -> None:
        """Close the underlying PostgreSQL snapshot reader."""
        self._reader.close()

    def _xml_table_counts(self) -> TableCounts:
        """Read completed XML import counters from the snapshot's durable progress table."""
        statement = sql.SQL("SELECT position, row_count, eof FROM {} WHERE table_name=%s").format(
            sql.Identifier(self._reader.schema, "_progress")
        )
        counts: list[tuple[str, int]] = []
        with self._reader.pool.connection() as connection:
            for table in IMPORTED_TABLES:
                row = connection.execute(statement, (table,)).fetchone()
                if row is None or row["eof"] is not True:
                    raise ValueError(f"Snapshot has no completed XML import counter for {table}")
                position, row_count = integer(row, "position"), integer(row, "row_count")
                if position != row_count or row_count < 0:
                    raise ValueError(f"Snapshot progress counter for {table} is inconsistent")
                counts.append((table, row_count))
        return tuple(counts)

    def _measured_table_counts(self) -> TableCounts:
        """Count every imported table in the pinned PostgreSQL schema."""
        counts: list[tuple[str, int]] = []
        with self._reader.pool.connection() as connection:
            for table in IMPORTED_TABLES:
                statement = sql.SQL("SELECT count(*) AS count FROM {}").format(
                    sql.Identifier(self._reader.schema, table)
                )
                counts.append((table, integer(connection.execute(statement).fetchone(), "count")))
        return tuple(counts)
