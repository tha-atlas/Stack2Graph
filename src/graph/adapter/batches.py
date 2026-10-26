"""Read a byte-bounded page and close its SQL stream before dependent queries."""

from collections.abc import Generator, Sequence
from contextlib import closing

from psycopg import sql

from sql.adapter.reader import PostgreSQLSnapshotReader
from sql.domain.models import Row


def bounded_page(
    reader: PostgreSQLSnapshotReader, query: sql.Composed, parameters: Sequence[int | list[str | int]], batch_rows: int
) -> tuple[Row, ...]:
    """Read the first bounded page and close the reader's batch generator."""
    rows = reader.iter_projected_rows(query, parameters, batch_rows)
    if not isinstance(rows, Generator):
        raise TypeError("Snapshot reader must provide a closeable batch generator")
    with closing(rows):
        return next(rows, ())
