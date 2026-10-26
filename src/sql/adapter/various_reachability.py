"""Traverse links using temporary tables without changing published data."""

from psycopg import Connection, sql

from selection.tags import TAG_TOKEN_PATTERN
from sql.adapter.postgres.postgres_store import table_sql
from sql.domain.models import Row
from sql.domain.rows import integer
from sql.domain.various_reachability import DepthCounts, ReachabilityResult

_SCRATCH_TABLES = ("audit_visited", "audit_frontier", "audit_next")


def tag_match(alias: str) -> sql.Composed:
    """Match whole source tokens against tag names classified by the Python selector."""
    return sql.SQL(
        "EXISTS (SELECT 1 FROM regexp_matches(COALESCE({}, ''), '" + TAG_TOKEN_PATTERN + "', 'g') "
        "AS token(value) WHERE token.value[1] = ANY(%s))"
    ).format(sql.Identifier(alias, "Tags"))


class VariousTraversal:
    """Store visited IDs and traversal frontiers in PostgreSQL temporary tables."""

    def __init__(self, connection: Connection[Row], schema: str) -> None:
        """Create temporary traversal tables on the supplied connection."""
        self.connection, self.schema = connection, schema
        # Only temporary scratch tables are created in this read-write setup transaction.
        # Subsequent reads and temporary INSERT/DELETE run in the reader's read-only mode.
        with connection.transaction():
            connection.execute("SET TRANSACTION READ WRITE")
            for name in _SCRATCH_TABLES:
                connection.execute(sql.SQL("CREATE TEMP TABLE {} (id bigint PRIMARY KEY)").format(sql.Identifier(name)))

    def _step_query(self, initial: bool) -> sql.Composed:
        """Build a breadth-first link step for selected roots or the frontier."""
        source = (
            sql.SQL('JOIN {} source ON source."Id"=link."PostId"').format(table_sql(self.schema, "Posts"))
            if initial
            else sql.SQL('JOIN pg_temp.audit_frontier source ON source.id=link."PostId"')
        )
        condition = sql.SQL('AND source."PostTypeId"=1 AND {}').format(tag_match("source")) if initial else sql.SQL("")
        return sql.SQL(
            """WITH candidates AS MATERIALIZED (
                   SELECT link."PostId" AS source_id, target."Id" AS target_id,
                          CASE WHEN target."Id" IS NULL THEN 'missing'
                               WHEN target."PostTypeId" IS DISTINCT FROM 1 THEN 'non_question'
                               WHEN {} THEN 'selected' ELSE 'various' END AS status
                   FROM {} link {} LEFT JOIN {} target ON target."Id"=link."RelatedPostId"
                   WHERE link."LinkTypeId"=1 {}
               ), added AS (
                   INSERT INTO pg_temp.audit_visited (id)
                   SELECT DISTINCT target_id FROM candidates WHERE status='various'
                   ON CONFLICT DO NOTHING RETURNING id
               ), frontier AS (
                   INSERT INTO pg_temp.audit_next (id) SELECT id FROM added RETURNING id
               )
               SELECT count(*) AS links, count(DISTINCT source_id) AS sources,
                      count(*) FILTER (WHERE status='various') AS various_links,
                      count(*) FILTER (WHERE status='selected') AS selected_links,
                      count(*) FILTER (WHERE status='missing') AS missing_links,
                      count(*) FILTER (WHERE status='non_question') AS non_question_links,
                      (SELECT count(*) FROM frontier) AS new_questions FROM candidates"""
        ).format(
            tag_match("target"), table_sql(self.schema, "PostLinks"), source, table_sql(self.schema, "Posts"), condition
        )

    def walk(self, language: str, source_tags: tuple[str, ...], global_tags: tuple[str, ...]) -> ReachabilityResult:
        """Traverse various-language links and return per-depth counts."""
        if not source_tags:
            return ReachabilityResult(language, ())
        for name in _SCRATCH_TABLES:
            self.connection.execute(sql.SQL("DELETE FROM {}").format(table_sql("pg_temp", name)))
        levels: list[DepthCounts] = []
        depth = 1
        while True:
            parameters = [list(global_tags), list(source_tags)] if depth == 1 else [list(global_tags)]
            row = self.connection.execute(self._step_query(depth == 1), parameters).fetchone()
            level = DepthCounts(
                depth,
                integer(row, "links"),
                integer(row, "sources"),
                integer(row, "various_links"),
                integer(row, "new_questions"),
                integer(row, "selected_links"),
                integer(row, "missing_links"),
                integer(row, "non_question_links"),
            )
            levels.append(level)
            if not level.new_questions:
                break
            self.connection.execute("DELETE FROM pg_temp.audit_frontier")
            self.connection.execute("INSERT INTO pg_temp.audit_frontier SELECT id FROM pg_temp.audit_next")
            self.connection.execute("DELETE FROM pg_temp.audit_next")
            depth += 1
        return ReachabilityResult(language, tuple(levels))
