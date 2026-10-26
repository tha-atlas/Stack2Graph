"""Read graph events from a fixed PostgreSQL snapshot through its reader."""

from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Final

from psycopg import Connection

from graph.adapter import queries
from graph.adapter.batches import bounded_page
from graph.adapter.decoding import (
    comment_record,
    payload,
    post_record,
    tag_record,
)
from graph.domain.events import (
    CommentEvent,
    LinkEvent,
    PostEvent,
    QuestionEnd,
    QuestionEvent,
    QuestionStart,
    SharedEvent,
)
from graph.domain.source import (
    IMPORTED_TABLES,
    DatabaseCapacity,
    PostReference,
    QuestionRange,
    SourceIntegrityError,
    SourceInventory,
    SourceProblem,
    TagUsage,
    VoteCounts,
    contradicts_observed_answers,
)
from graph.mapping.identity import IriPolicy
from selection.languages import LanguageSelector
from sql.adapter.reader import PostgreSQLSnapshotReader
from sql.domain.models import Row
from sql.domain.postgres import PostgreSQLConfig
from sql.domain.rows import integer, optional_integer, text
from utils.config import DataPaths, SnapshotPaths
from utils.logging_utils import get_logger
from utils.resources import BatchLimits, ResourcePlan

LOGGER = get_logger(__name__)
INVENTORY_PARALLEL_WORKERS: Final = 8
RANGE_COUNT_WORKERS: Final = 64


def _parallel_query_workers(cpu_workers: int, server_workers: int) -> int:
    """Limit query workers to available CPUs and PostgreSQL capacity."""
    if cpu_workers < 1 or server_workers < 0:
        raise ValueError("Parallel query limits are invalid")
    return min(cpu_workers, server_workers)


def _id_ranges(first: int, last: int, workers: int) -> tuple[tuple[int, int], ...]:
    """Split inclusive ID bounds into at most workers half-open ranges."""
    if first < 0 or last < first or workers < 1:
        raise ValueError("ID range limits are invalid")
    end = last + 1
    width = max(1, (end - first + workers - 1) // workers)
    return tuple((start, min(end, start + width)) for start in range(first, end, width))


def fail(code: str, table: str, source_id: int, field: str, target: int | None, message: str) -> None:
    """Raise a source integrity error containing one structured problem."""
    raise SourceIntegrityError((SourceProblem(code, table, source_id, field, target, message),))


class PostgreSQLGraphSource:
    def __init__(
        self,
        paths: DataPaths | SnapshotPaths,
        resources: ResourcePlan,
        config: PostgreSQLConfig,
        selector: LanguageSelector,
        iris: IriPolicy,
        *,
        snapshot_id: str,
    ) -> None:
        """Open a pinned snapshot reader with the supplied selector and IRI policy."""
        self.reader = PostgreSQLSnapshotReader(paths, resources, config, snapshot_id=snapshot_id)
        self.selector, self.iris = selector, iris

    def describe(self) -> SourceInventory:
        """Read published table counts, question bounds, and unresolved references."""
        self.reader.check_process()
        expected = dict(self.reader.validation.counts)
        if set(expected) != set(IMPORTED_TABLES):
            raise ValueError("Graph export requires all five fully published source tables")
        with self.reader.pool.connection() as connection, connection.transaction():
            LOGGER.info("Reading indexed question bounds and count")
            connection.execute(
                "SELECT set_config('max_parallel_workers_per_gather', "
                "least(current_setting('max_parallel_workers')::integer, %s)::text, true)",
                (INVENTORY_PARALLEL_WORKERS,),
            )
            row = connection.execute(queries.query(self.reader.schema, queries.QUESTION_INVENTORY)).fetchone()
            if row is None:
                raise ValueError("Source inventory missing")
        return SourceInventory(
            self.reader.snapshot_identity,
            self.reader.validation.counts,
            integer(row, "count"),
            optional_integer(row, "first"),
            optional_integer(row, "last"),
            (
                ("unresolved_tag_memberships", integer(row, "unresolved_tag_memberships")),
                ("unresolved_tag_posts", integer(row, "unresolved_tag_posts")),
                ("unresolved_tag_comments", integer(row, "unresolved_tag_comments")),
            ),
        )

    def database_capacity(self) -> DatabaseCapacity:
        """Read available nonreserved database connection slots."""
        self.reader.check_process()
        with self.reader.pool.connection() as connection:
            row = connection.execute(
                "SELECT current_setting('max_connections')::bigint-current_setting('superuser_reserved_connections')::bigint-current_setting('reserved_connections')::bigint-(SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid()) AS slots"
            ).fetchone()
        slots = integer(row, "slots")
        if slots < 1:
            raise ValueError("No nonreserved PostgreSQL connection headroom")
        return DatabaseCapacity(slots)

    def export_table_counts(self) -> tuple[tuple[str, int], ...]:
        """Count source rows accepted by the export filters in PostgreSQL."""
        self.reader.check_process()
        with self.reader.pool.connection() as connection, connection.transaction():
            settings = connection.execute(
                "SELECT current_setting('max_parallel_workers')::integer AS parallel_workers"
            ).fetchone()
            if settings is None:
                raise ValueError("PostgreSQL parallel query settings missing")
            parallel_workers = _parallel_query_workers(
                self.reader.resources.threads,
                integer(settings, "parallel_workers"),
            )
            connection.execute(
                "SELECT set_config('max_parallel_workers_per_gather', %s, true)",
                (str(parallel_workers),),
            )
            unresolved_rows = connection.execute(
                queries.query(self.reader.schema, queries.UNRESOLVED_QUESTION_IDS)
            ).fetchall()
            bounds = connection.execute(queries.query(self.reader.schema, queries.POST_ID_BOUNDS)).fetchone()
            tag_row = connection.execute(queries.query(self.reader.schema, queries.TAGS_ROW_COUNT))
            tag_count = tag_row.fetchone()
        if bounds is None or tag_count is None:
            raise ValueError("Expected export count bounds missing")
        unresolved = tuple(integer(row, "Id") for row in unresolved_rows)
        ranges = _id_ranges(integer(bounds, "first"), integer(bounds, "last"), RANGE_COUNT_WORKERS)
        query_workers = min(RANGE_COUNT_WORKERS, self.reader.resources.threads, self.reader.config.pool_size)
        LOGGER.info(
            "Counting expected export rows in %d ID ranges with %d concurrent PostgreSQL queries",
            len(ranges),
            query_workers,
        )
        counts = dict.fromkeys(IMPORTED_TABLES, 0)
        counts["Tags"] = integer(tag_count, "count")
        remaining = {table: len(ranges) for table, _ in queries.EXPORT_RANGE_COUNT_QUERIES}
        LOGGER.info("Counted expected Tags rows: %d", counts["Tags"])
        active_connections: list[Connection[Row]] = []
        active_lock = Lock()
        with ThreadPoolExecutor(max_workers=query_workers, thread_name_prefix="export-count") as executor:
            futures = tuple(
                executor.submit(
                    self._export_range_count,
                    table,
                    template,
                    unresolved,
                    first,
                    end,
                    active_connections,
                    active_lock,
                )
                for table, template in queries.EXPORT_RANGE_COUNT_QUERIES
                for first, end in ranges
            )
            try:
                for future in as_completed(futures):
                    table, count = future.result()
                    counts[table] += count
                    remaining[table] -= 1
                    if remaining[table] == 0:
                        LOGGER.info("Counted expected %s rows: %d", table, counts[table])
            except BaseException:
                for future in futures:
                    future.cancel()
                with active_lock:
                    connections = tuple(active_connections)
                for connection in connections:
                    connection.cancel_safe(timeout=5.0)
                raise
        return tuple((table, counts[table]) for table in IMPORTED_TABLES)

    def _export_range_count(
        self,
        table: str,
        template: str,
        unresolved: tuple[int, ...],
        first: int,
        end: int,
        active_connections: list[Connection[Row]],
        active_lock: Lock,
    ) -> tuple[str, int]:
        """Count exportable rows in one range while tracking cancellable queries."""
        with self.reader.pool.connection() as connection, connection.transaction():
            with active_lock:
                active_connections.append(connection)
            try:
                connection.execute("SELECT set_config('max_parallel_workers_per_gather', '0', true)")
                unresolved_parameter: list[str | int] = list(unresolved)
                parameters: tuple[list[str | int], int, int] = (unresolved_parameter, first, end)
                row = connection.execute(
                    queries.query(self.reader.schema, template),
                    parameters,
                ).fetchone()
            finally:
                with active_lock:
                    active_connections.remove(connection)
        if row is None:
            raise ValueError(f"Expected {table} export count missing")
        return table, integer(row, "count")

    def iter_questions(self, part: QuestionRange, limits: BatchLimits) -> Iterator[QuestionEvent]:
        """Yield bounded question events with their owned records and links."""
        if (
            limits.max_rows > self.reader.resources.batches.max_rows
            or limits.max_bytes > self.reader.resources.batches.max_bytes
        ):
            raise ValueError("Graph source batch exceeds reader budget")
        first = part.first_id
        while first < part.end_id:
            pages = bounded_page(
                self.reader,
                queries.query(self.reader.schema, queries.QUESTION_PAGE),
                (first, part.end_id, limits.max_rows),
                limits.max_rows,
            )
            if not pages:
                return
            posts = tuple(post_record(payload(text(row, "payload"))) for row in pages)
            names: list[str | int] = sorted({tag for post in posts for tag in post.tags})
            tags = {
                tag.name: tag
                for batch in self.reader.iter_projected_rows(
                    queries.query(
                        self.reader.schema,
                        f"SELECT {queries.cast_payload('t')} AS payload FROM {{Tags}} t WHERE "
                        '"TagName"=ANY(%s) ORDER BY "Id"',
                    ),
                    (names,),
                    limits.max_rows,
                )
                for row in batch
                for tag in (tag_record(payload(text(row, "payload"))),)
            }
            starts: dict[int, QuestionStart] = {}
            for post, row in zip(posts, pages, strict=True):
                self.iris.question(post.id)
                for name in post.tags:
                    if name not in tags:
                        fail("missing_tag", "Posts", post.id, "Tags", None, f"Missing Tags row for {name}")
                accepted = None
                if post.accepted_answer_id is not None:
                    accepted = PostReference(
                        post.accepted_answer_id,
                        row.get("accepted_exists") is not None,
                        optional_integer(row, "accepted_type"),
                        optional_integer(row, "accepted_parent"),
                    )
                observed = integer(row, "observed_answers")
                if contradicts_observed_answers(post.answer_count, observed):
                    fail(
                        "contradictory_answer_count",
                        "Posts",
                        post.id,
                        "AnswerCount",
                        None,
                        "Answer evidence contradicts source count",
                    )
                graphs = self.selector.match(post.tags) or ("extra-languages",)
                starts[post.id] = QuestionStart(
                    post,
                    tuple(tags[name] for name in sorted(set(post.tags))),
                    VoteCounts(0, 0),
                    observed,
                    accepted,
                    graphs,
                )
            ids: list[str | int] = [post.id for post in posts]
            for batch in self.reader.iter_projected_rows(
                queries.query(self.reader.schema, queries.QUESTION_EVENTS), (ids,), limits.max_rows
            ):
                for row in batch:
                    owner, rank = integer(row, "owner"), integer(row, "rank")
                    start = starts[owner]
                    votes = VoteCounts(integer(row, "up"), integer(row, "down"))
                    if rank == 0:
                        yield QuestionStart(
                            start.post, start.tags, votes, start.observed_answers, start.accepted, start.graph_keys
                        )
                    elif rank == 4:
                        yield QuestionEnd(owner)
                    else:
                        source_row = payload(text(row, "payload"))
                        if rank == 1:
                            post = post_record(source_row)
                            yield PostEvent(post, votes)
                        elif rank == 2:
                            comment = comment_record(source_row)
                            parent = PostReference(
                                comment.post_id,
                                True,
                                optional_integer(row, "parent_type"),
                                optional_integer(row, "parent_question"),
                            )
                            yield CommentEvent(comment, parent)
                        elif rank == 3:
                            # The shared export predicate already restricts both link endpoints
                            # to exported questions, so every row yields an edge.
                            target = PostReference(
                                integer(source_row, "RelatedPostId"),
                                True,
                                optional_integer(row, "parent_type"),
                                optional_integer(row, "parent_question"),
                            )
                            yield LinkEvent(
                                owner, target, integer(source_row, "LinkTypeId"), integer(source_row, "source_rows")
                            )
                        else:
                            raise ValueError("Unexpected graph source event kind")
            first = posts[-1].id + 1

    def iter_shared(self, usages: Iterable[TagUsage], limits: BatchLimits) -> Iterator[SharedEvent]:
        """Yield shared tag and documentation events under the batch limits."""
        from graph.adapter.shared_source import iter_shared

        yield from iter_shared(self, usages, limits)

    def close(self) -> None:
        """Close the underlying PostgreSQL snapshot reader."""
        self.reader.close()
