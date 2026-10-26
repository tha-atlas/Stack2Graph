"""Inspect directed type-1 PostLinks through bounded, read-only queries."""

from collections.abc import Iterator

from psycopg import sql

from sql.adapter.postgres.postgres_store import table_sql
from sql.adapter.reader import PostgreSQLSnapshotReader
from sql.domain.models import Row
from sql.domain.rows import integer


def linked_groups_query(schema: str) -> sql.Composed:
    """Count source link records once, retaining missing and non-question targets."""
    return sql.SQL(
        '''SELECT source."PostTypeId" AS source_type, source."Tags" AS source_tags,
                      CASE WHEN target."Id" IS NULL THEN 0 ELSE 1 END AS target_exists,
                      target."PostTypeId" AS target_type, target."Tags" AS target_tags,
                      count(*) AS links
               FROM {} link
               LEFT JOIN {} source ON source."Id" = link."PostId"
               LEFT JOIN {} target ON target."Id" = link."RelatedPostId"
               WHERE link."LinkTypeId" = 1
               GROUP BY source."PostTypeId", source."Tags", target."Id" IS NULL,
                        target."PostTypeId", target."Tags"'''
    ).format(table_sql(schema, "PostLinks"), table_sql(schema, "Posts"), table_sql(schema, "Posts"))


class LinkedPostReader(PostgreSQLSnapshotReader):
    """Read published snapshot links within read-only sessions and byte limits."""

    def count_links(self) -> int:
        """Return the exact type-1 record count for the progress display."""
        self._check_process()
        with self.pool.connection() as connection:
            row = connection.execute(
                sql.SQL('SELECT count(*) AS links FROM {} WHERE "LinkTypeId"=1').format(
                    table_sql(self.schema, "PostLinks")
                )
            ).fetchone()
        return integer(row, "links")

    def iter_link_groups(self, batch_rows: int = 1024) -> Iterator[tuple[Row, ...]]:
        """Group equal tag/type combinations with bounded client memory."""
        yield from self._rows(linked_groups_query(self.schema), [], batch_rows)
