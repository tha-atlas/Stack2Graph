"""
Build read-only queries over fixed source ranges and record IDs.

Every exportability predicate lives here exactly once so export queries and
expected-count queries share identical filtering semantics.
"""

from psycopg import sql

from selection.tags import TAG_TOKEN_PATTERN


def query(schema: str, template: str) -> sql.Composed:
    """Bind source table placeholders to quoted schema-qualified identifiers."""
    return sql.SQL(template).format(
        **{name: sql.Identifier(schema, name) for name in ("Posts", "Comments", "Tags", "Votes", "PostLinks")}
    )


EXPORT_LINK_TYPES = (1, 3)
DOCUMENTATION_REFERENCES = (("WikiPostId", 5), ("ExcerptPostId", 4))


def link_type_filter(alias: str) -> str:
    """Build the SQL predicate for exported post-link types."""
    return f'{alias}."LinkTypeId" IN ({",".join(str(link_type) for link_type in EXPORT_LINK_TYPES)})'


def aggregated_vote_type(alias: str) -> str:
    """Build the SQL predicate for upvotes and downvotes."""
    return f'{alias}."VoteTypeId" IN (2,3)'


def resolved_question_tags(alias: str) -> str:
    """Build a predicate requiring every question tag to exist in Tags."""
    return f"""NOT EXISTS (
 SELECT 1 FROM regexp_matches(coalesce({alias}."Tags",''),'{TAG_TOKEN_PATTERN}','g') AS token(name)
 WHERE NOT EXISTS (SELECT 1 FROM {{Tags}} t WHERE t."TagName"=token.name[1])
)"""


def exportable_question(alias: str) -> str:
    """Build a predicate for question posts with resolved tag names."""
    return f'({alias}."PostTypeId"=1 AND {resolved_question_tags(alias)})'


def referenced_documentation(alias: str) -> str:
    """Build a predicate for correctly typed tag documentation references."""
    matches = " OR ".join(
        f't."{column}"={alias}."Id" AND {alias}."PostTypeId"={expected_type}'
        for column, expected_type in DOCUMENTATION_REFERENCES
    )
    return f"(EXISTS (SELECT 1 FROM {{Tags}} t WHERE {matches}))"


def orphan_documentation(alias: str) -> str:
    """Build a predicate for documentation posts with no owning tag."""
    return f"""({alias}."PostTypeId" IN (4,5) AND NOT EXISTS (
 SELECT 1 FROM {{Tags}} t WHERE t."WikiPostId"={alias}."Id" OR t."ExcerptPostId"={alias}."Id"))"""


def documentation_owner_rows(source_alias: str) -> str:
    """Build SQL rows pairing tag owners with their documentation posts."""
    return " UNION ALL ".join(
        f'SELECT "Id" AS owner,"{column}" AS doc,{expected_type} AS expected_type '
        f'FROM {source_alias} WHERE "{column}" IS NOT NULL'
        for column, expected_type in DOCUMENTATION_REFERENCES
    )


_VOTES_AGGREGATE = (
    'SELECT v."PostId",count(*) FILTER (WHERE v."VoteTypeId"=2) AS up',
    'count(*) FILTER (WHERE v."VoteTypeId"=3) AS down FROM {Votes} v',
    'JOIN owned o ON o."Id"=v."PostId" GROUP BY v."PostId"',
)


def votes_cte() -> str:
    """Build compact vote totals for the caller's CTE list."""
    first, second, third = _VOTES_AGGREGATE
    return f"votes AS ({first},{second} {third})"


def cast_payload(alias: str, *dropped: str) -> str:
    """Project one row as JSONB text without audit and joiner columns."""
    columns = "".join(f"-'{column}'" for column in ("ExtraAttributes", *dropped))
    return f"(to_jsonb({alias}){columns})::text"


def join_owned_votes() -> str:
    """Build a vote lookup against the owned-events CTE."""
    return 'FROM owned o LEFT JOIN votes v ON v."PostId"=o."Id"'


def join_owned_comments() -> str:
    """Build a comment lookup against the owned-events CTE."""
    return 'FROM {Comments} c JOIN owned o ON o."Id"=c."PostId"'


UNRESOLVED_QUESTIONS = """unresolved_questions AS MATERIALIZED (
 SELECT DISTINCT q."Id"
 FROM {Posts} q
 CROSS JOIN LATERAL regexp_matches(coalesce(q."Tags",''),'{pattern}','g') AS token(name)
 LEFT JOIN {Tags} t ON t."TagName"=token.name[1]
 WHERE q."PostTypeId"=1 AND t."Id" IS NULL
)""".replace("{pattern}", TAG_TOKEN_PATTERN)


QUESTION_INVENTORY = (
    "\nWITH "
    + UNRESOLVED_QUESTIONS
    + """, unresolved_posts AS MATERIALIZED (
 SELECT u."Id" AS post_id FROM unresolved_questions u
 UNION ALL
 SELECT a."Id" FROM {Posts} a JOIN unresolved_questions u ON u."Id"=a."ParentId"
 WHERE a."PostTypeId"=2
)
SELECT count(*) FILTER (WHERE u."Id" IS NULL) AS count,
 min(q."Id") FILTER (WHERE u."Id" IS NULL) AS first,
 max(q."Id") FILTER (WHERE u."Id" IS NULL) AS last,
 (SELECT count(*) FROM unresolved_questions) AS unresolved_tag_memberships,
 (SELECT count(*) FROM unresolved_posts) AS unresolved_tag_posts,
 (SELECT count(*) FROM {Comments} c JOIN unresolved_posts p ON p.post_id=c."PostId")
 AS unresolved_tag_comments
FROM {Posts} q LEFT JOIN unresolved_questions u ON u."Id"=q."Id" WHERE q."PostTypeId"=1
"""
)


GRAPH_QUESTION_IDS = (
    """
SELECT q."Id" FROM {Posts} q WHERE q."Id">%s AND """
    + exportable_question("q")
    + """ ORDER BY q."Id" LIMIT %s
"""
)

RAW_QUESTION_COUNT = 'SELECT count(*) AS count FROM {Posts} q WHERE q."PostTypeId"=1 AND q."Id">%s'


QUESTION_PAGE = (
    """
WITH q AS MATERIALIZED (SELECT * FROM {Posts} q WHERE q."Id">=%s AND q."Id"<%s AND """
    + exportable_question("q")
    + """ ORDER BY q."Id" LIMIT %s),
counts AS (SELECT a."ParentId",count(*) AS observed_answers FROM {Posts} a JOIN q ON q."Id"=a."ParentId" WHERE a."PostTypeId"=2 GROUP BY a."ParentId")
SELECT q."Id" AS source_id,'Posts'::text AS source_table,"""
    + cast_payload("q")
    + """ AS payload,coalesce(counts.observed_answers,0) AS observed_answers,
accepted."Id" AS accepted_exists,accepted."PostTypeId" AS accepted_type,accepted."ParentId" AS accepted_parent
FROM q LEFT JOIN counts ON counts."ParentId"=q."Id" LEFT JOIN {Posts} accepted ON accepted."Id"=q."AcceptedAnswerId" ORDER BY q."Id"
"""
)

# The question batch alone materializes the vote totals; this query owns the wrapper
# and its CTE-list comma, while the aggregate body stays shared.
_VOTES_CTE_MATERIALIZED = (
    ", votes AS MATERIALIZED (\n "
    + _VOTES_AGGREGATE[0]
    + ",\n "
    + _VOTES_AGGREGATE[1]
    + "\n "
    + _VOTES_AGGREGATE[2]
    + "\n)"
)

QUESTION_EVENTS = (
    """
WITH q AS MATERIALIZED (SELECT "Id" FROM {Posts} WHERE "Id"=ANY(%s)),
owned AS MATERIALIZED (
 SELECT p.*,p."Id" AS owner FROM {Posts} p JOIN q ON q."Id"=p."Id"
 UNION ALL SELECT a.*,a."ParentId" AS owner FROM {Posts} a JOIN q ON q."Id"=a."ParentId" WHERE a."PostTypeId"=2
)"""
    + _VOTES_CTE_MATERIALIZED
    + """, events AS (
 SELECT o.owner,0 AS rank,o."Id" AS event_id,NULL::text AS payload,
 NULL::bigint AS parent_type,NULL::bigint AS parent_question,NULL::bigint AS target_exists,
 coalesce(v.up,0) AS up,coalesce(v.down,0) AS down
 """
    + join_owned_votes()
    + """ WHERE o."PostTypeId"=1
 UNION ALL
 SELECT o.owner,1,o."Id","""
    + cast_payload("o", "owner")
    + """,NULL,NULL,NULL,coalesce(v.up,0),coalesce(v.down,0)
 """
    + join_owned_votes()
    + """ WHERE o."PostTypeId"=2
 UNION ALL
 SELECT o.owner,2,c."Id","""
    + cast_payload("c")
    + """,o."PostTypeId",o."ParentId",o."Id",0,0
 """
    + join_owned_comments()
    + """
 UNION ALL
 SELECT l."PostId",3,l."RelatedPostId",jsonb_build_object('PostId',l."PostId",'RelatedPostId',l."RelatedPostId",'LinkTypeId',l."LinkTypeId",'source_rows',count(*))::text,
 target."PostTypeId",target."ParentId",target."Id",0,0
 FROM {PostLinks} l JOIN q ON q."Id"=l."PostId" JOIN {Posts} target ON target."Id"=l."RelatedPostId" AND """
    + exportable_question("target")
    + """
 WHERE """
    + link_type_filter("l")
    + """
 GROUP BY l."PostId",l."RelatedPostId",l."LinkTypeId",target."Id",target."PostTypeId",target."ParentId"
 UNION ALL SELECT q."Id",4,q."Id",NULL,NULL,NULL,NULL,0,0 FROM q
) SELECT *,event_id AS source_id,CASE WHEN rank=2 THEN 'Comments' WHEN rank=3 THEN 'PostLinks' ELSE 'Posts' END AS source_table FROM events ORDER BY owner,rank,event_id,payload
"""
)

EXPORT_QUESTION_SCOPE = (
    "\nWITH "
    + UNRESOLVED_QUESTIONS
    + """, exported_questions AS NOT MATERIALIZED (
 SELECT q."Id" FROM {Posts} q
 LEFT JOIN unresolved_questions u ON u."Id"=q."Id"
 WHERE q."PostTypeId"=1 AND u."Id" IS NULL
)
"""
)

EXPORT_POST_SCOPE = (
    EXPORT_QUESTION_SCOPE
    + """, exported_posts AS NOT MATERIALIZED (
 SELECT eq."Id" FROM exported_questions eq
 UNION ALL
 SELECT a."Id" FROM {Posts} a JOIN exported_questions eq ON eq."Id"=a."ParentId"
 WHERE a."PostTypeId"=2
 UNION ALL
 SELECT p."Id" FROM {Posts} p WHERE """
    + referenced_documentation("p")
    + """ OR """
    + orphan_documentation("p")
    + """
)
"""
)

TAGS_ROW_COUNT = "SELECT count(*) AS count FROM {Tags}"

EXPORT_TABLE_COUNT_QUERIES = (
    ("Posts", EXPORT_POST_SCOPE + "SELECT count(*) AS count FROM exported_posts"),
    (
        "Comments",
        EXPORT_POST_SCOPE
        + """SELECT count(*) AS count FROM {Comments} c
 WHERE EXISTS (SELECT 1 FROM exported_posts ep WHERE ep."Id"=c."PostId")""",
    ),
    (
        "Votes",
        EXPORT_POST_SCOPE
        + "SELECT count(*) AS count FROM {Votes} v WHERE "
        + aggregated_vote_type("v")
        + """ AND EXISTS (SELECT 1 FROM exported_posts ep WHERE ep."Id"=v."PostId")""",
    ),
    ("Tags", TAGS_ROW_COUNT),
    (
        "PostLinks",
        EXPORT_QUESTION_SCOPE
        + "SELECT count(*) AS count FROM {PostLinks} l WHERE "
        + link_type_filter("l")
        + """ AND EXISTS (SELECT 1 FROM exported_questions eo WHERE eo."Id"=l."PostId")
 AND EXISTS (SELECT 1 FROM exported_questions et WHERE et."Id"=l."RelatedPostId")""",
    ),
)


def exported_post_in_range(alias: str) -> str:
    """Build the exportability predicate for posts in a source range."""
    valid_question = f'({alias}."PostTypeId"=1 AND NOT EXISTS (SELECT 1 FROM unresolved u WHERE u.id={alias}."Id"))'
    valid_answer = f"""({alias}."PostTypeId"=2 AND EXISTS (
 SELECT 1 FROM {{Posts}} q WHERE q."Id"={alias}."ParentId" AND q."PostTypeId"=1
 AND NOT EXISTS (SELECT 1 FROM unresolved u WHERE u.id=q."Id")))"""
    return (
        f"({valid_question} OR {valid_answer} OR "
        + referenced_documentation(alias)
        + " OR "
        + orphan_documentation(alias)
        + ")"
    )


UNRESOLVED_QUESTION_IDS = "\nWITH " + UNRESOLVED_QUESTIONS + ' SELECT "Id" FROM unresolved_questions'
POST_ID_BOUNDS = 'SELECT min("Id") AS first,max("Id") AS last FROM {Posts}'
RANGE_UNRESOLVED_SCOPE = "WITH unresolved AS MATERIALIZED (SELECT unnest(%s::bigint[]) AS id) "
EXPORT_RANGE_COUNT_QUERIES = (
    (
        "Posts",
        RANGE_UNRESOLVED_SCOPE
        + 'SELECT count(*) AS count FROM {Posts} p WHERE p."Id">=%s AND p."Id"<%s AND '
        + exported_post_in_range("p"),
    ),
    (
        "Comments",
        RANGE_UNRESOLVED_SCOPE
        + 'SELECT count(*) AS count FROM {Comments} c JOIN {Posts} p ON p."Id"=c."PostId" '
        + 'WHERE c."PostId">=%s AND c."PostId"<%s AND '
        + exported_post_in_range("p"),
    ),
    (
        "Votes",
        RANGE_UNRESOLVED_SCOPE
        + 'SELECT count(*) AS count FROM {Votes} v JOIN {Posts} p ON p."Id"=v."PostId" '
        + 'WHERE v."PostId">=%s AND v."PostId"<%s AND '
        + aggregated_vote_type("v")
        + " AND "
        + exported_post_in_range("p"),
    ),
    (
        "PostLinks",
        RANGE_UNRESOLVED_SCOPE
        + 'SELECT count(*) AS count FROM {PostLinks} l JOIN {Posts} source ON source."Id"=l."PostId" '
        + 'JOIN {Posts} target ON target."Id"=l."RelatedPostId" '
        + 'WHERE l."PostId">=%s AND l."PostId"<%s AND '
        + link_type_filter("l")
        + ' AND source."PostTypeId"=1 AND target."PostTypeId"=1 '
        + 'AND NOT EXISTS (SELECT 1 FROM unresolved u WHERE u.id=source."Id") '
        + 'AND NOT EXISTS (SELECT 1 FROM unresolved u WHERE u.id=target."Id")',
    ),
)
