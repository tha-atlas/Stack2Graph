"""Read shared tags and documentation through one connection in bounded pages."""

from collections.abc import Iterable, Iterator
from typing import Protocol

from graph.adapter import queries
from graph.adapter.batches import bounded_page
from graph.adapter.decoding import (
    comment_record,
    payload,
    post_record,
    tag_record,
)
from graph.domain.events import CommentEvent, PostEvent, SharedEnd, SharedEvent, SharedStart
from graph.domain.source import PostReference, TagRecord, TagUsage, VoteCounts
from graph.mapping.identity import IriPolicy
from sql.adapter.reader import PostgreSQLSnapshotReader
from sql.domain.rows import integer, optional_integer, text
from utils.resources import BatchLimits


class SharedSourceAccess(Protocol):
    reader: PostgreSQLSnapshotReader
    iris: IriPolicy


SHARED_EVENTS = (
    """
WITH t AS MATERIALIZED (SELECT * FROM {Tags} WHERE "Id"=ANY(%s)),
owners AS MATERIALIZED (
"""
    + queries.documentation_owner_rows("t")
    + """
), owned AS MATERIALIZED (
 SELECT p.*,o.owner FROM owners o JOIN {Posts} p ON p."Id"=o.doc AND p."PostTypeId"=o.expected_type
),
"""
    + queries.votes_cte()
    + """,
events AS (
 SELECT "Id" AS owner,0::bigint AS doc,0 AS rank,"Id" AS event_id,NULL::text AS payload,NULL::bigint AS parent_type,0::bigint AS up,0::bigint AS down FROM t
 UNION ALL SELECT o.owner,o."Id",1,o."Id","""
    + queries.cast_payload("o", "owner")
    + """,NULL,coalesce(v.up,0),coalesce(v.down,0) """
    + queries.join_owned_votes()
    + """
 UNION ALL SELECT o.owner,o."Id",2,c."Id","""
    + queries.cast_payload("c")
    + """,o."PostTypeId",0,0 """
    + queries.join_owned_comments()
    + """
 UNION ALL SELECT "Id",9223372036854775807,4,"Id",NULL,NULL,0,0 FROM t
) SELECT *,event_id AS source_id,CASE WHEN rank=2 THEN 'Comments' ELSE 'Posts' END AS source_table FROM events ORDER BY owner,doc,rank,event_id
"""
)

ORPHAN_EVENTS = (
    """
WITH owned AS MATERIALIZED (SELECT p.*,p."Id" AS owner FROM {Posts} p WHERE p."Id"=ANY(%s)),
"""
    + queries.votes_cte()
    + """,
events AS (
 SELECT owner,0 AS rank,owner AS event_id,NULL::text AS payload,NULL::bigint AS parent_type,0::bigint AS up,0::bigint AS down FROM owned
 UNION ALL SELECT o.owner,1,o."Id","""
    + queries.cast_payload("o", "owner")
    + """,NULL,coalesce(v.up,0),coalesce(v.down,0) """
    + queries.join_owned_votes()
    + """
 UNION ALL SELECT o.owner,2,c."Id","""
    + queries.cast_payload("c")
    + """,o."PostTypeId",0,0 """
    + queries.join_owned_comments()
    + """
 UNION ALL SELECT owner,4,owner,NULL,NULL,0,0 FROM owned
) SELECT *,event_id AS source_id,CASE WHEN rank=2 THEN 'Comments' ELSE 'Posts' END AS source_table FROM events ORDER BY owner,rank,event_id
"""
)


def _events(
    source: SharedSourceAccess,
    owners: dict[int, tuple[TagRecord | None, tuple[str, ...]]],
    template: str,
    limits: BatchLimits,
) -> Iterator[SharedEvent]:
    """Decode ordered shared-record rows into tag and documentation events."""
    ids: list[str | int] = list(owners)
    for batch in source.reader.iter_projected_rows(
        queries.query(source.reader.schema, template), (ids,), limits.max_rows
    ):
        for row in batch:
            owner, rank = integer(row, "owner"), integer(row, "rank")
            tag, graphs = owners[owner]
            if rank == 0:
                yield SharedStart(tag, graphs)
            elif rank == 4:
                yield SharedEnd()
            else:
                fields = payload(text(row, "payload"))
                if rank == 1:
                    post = post_record(fields)
                    yield PostEvent(post, VoteCounts(integer(row, "up"), integer(row, "down")))
                elif rank == 2:
                    comment = comment_record(fields)
                    parent = PostReference(comment.post_id, True, optional_integer(row, "parent_type"), None)
                    if parent.post_type is None:
                        raise ValueError("Documentation comment parent type missing")
                    yield CommentEvent(comment, parent)
                else:
                    raise ValueError("Unknown shared source event")


def iter_shared(source: SharedSourceAccess, usages: Iterable[TagUsage], limits: BatchLimits) -> Iterator[SharedEvent]:
    """Yield shared tag documentation and orphan events in bounded batches."""
    usage = iter(usages)
    pending = next(usage, None)
    previous: TagUsage | None = None
    last_id = 0
    while True:
        rows = bounded_page(
            source.reader,
            queries.query(
                source.reader.schema,
                "SELECT t.\"Id\" AS source_id,'Tags'::text AS source_table,"
                f'{queries.cast_payload("t")} AS payload FROM {{Tags}} t WHERE "Id">%s ORDER BY "Id" LIMIT %s',
            ),
            (last_id, limits.max_rows),
            limits.max_rows,
        )
        if not rows:
            break
        owners: dict[int, tuple[TagRecord | None, tuple[str, ...]]] = {}
        for row in rows:
            tag = tag_record(payload(text(row, "payload")))
            graphs: set[str] = set()
            if pending is not None and pending.tag_id < tag.id:
                raise ValueError("Tag usage references missing source tag")
            while pending is not None and pending.tag_id == tag.id:
                if previous is not None and pending < previous:
                    raise ValueError("Tag usage must be sorted")
                graphs.add(pending.graph_key)
                previous = pending
                pending = next(usage, None)
            owners[tag.id] = (tag, tuple(sorted(graphs)) or ("extra-languages",))
            last_id = tag.id
        yield from _events(source, owners, SHARED_EVENTS, limits)
    if pending is not None:
        raise ValueError("Tag usage references missing source tag")
    last_id = 0
    while True:
        template = (
            """SELECT p."Id" FROM {Posts} p WHERE """
            + queries.orphan_documentation("p")
            + """ AND p."Id">%s ORDER BY p."Id" LIMIT %s"""
        )
        rows = bounded_page(
            source.reader, queries.query(source.reader.schema, template), (last_id, limits.max_rows), limits.max_rows
        )
        if not rows:
            return
        orphans: dict[int, tuple[TagRecord | None, tuple[str, ...]]] = {
            integer(row, "Id"): (None, ("extra-languages",)) for row in rows
        }
        yield from _events(source, orphans, ORPHAN_EVENTS, limits)
        last_id = integer(rows[-1], "Id")
