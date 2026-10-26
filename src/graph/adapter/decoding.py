"""Validate and decode source row fields without discarding their values."""

from graph.domain.source import (
    CommentRecord,
    PostDates,
    PostRecord,
    SourceIntegrityError,
    SourceProblem,
    TagRecord,
)
from selection.tags import parse_tags
from sql.domain.models import Row
from sql.domain.rows import integer, optional_integer, optional_text, text
from utils.types import json_object, parse_json


def payload(value: str) -> Row:
    """Decode a JSON source projection, rejecting nonscalar values."""
    fields = json_object(parse_json(value))
    row: Row = {}
    for name, scalar in fields.items():
        if scalar is None or isinstance(scalar, (str, int, bool)):
            row[name] = scalar
        else:
            raise ValueError(f"Unexpected nonscalar source projection: {name}")
    return row


def post_record(row: Row) -> PostRecord:
    """Decode a post row and report malformed tags as source integrity errors."""
    try:
        tags = parse_tags(optional_text(row, "Tags"))
    except ValueError as error:
        raise SourceIntegrityError(
            (SourceProblem("invalid_tags", "Posts", integer(row, "Id"), "Tags", None, "Malformed tag tokens"),)
        ) from error
    return PostRecord(
        integer(row, "Id"),
        integer(row, "PostTypeId"),
        optional_integer(row, "ParentId"),
        optional_text(row, "Title"),
        optional_text(row, "Body"),
        tags,
        optional_integer(row, "Score"),
        optional_integer(row, "ViewCount"),
        optional_integer(row, "AnswerCount"),
        optional_integer(row, "CommentCount"),
        optional_integer(row, "FavoriteCount"),
        optional_integer(row, "AcceptedAnswerId"),
        PostDates(
            *(
                optional_text(row, key)
                for key in ("CreationDate", "LastEditDate", "LastActivityDate", "ClosedDate", "CommunityOwnedDate")
            )
        ),
        optional_text(row, "ContentLicense"),
    )


def comment_record(row: Row) -> CommentRecord:
    """Decode a comment row with optional text, score, date, and license."""
    return CommentRecord(
        integer(row, "Id"),
        integer(row, "PostId"),
        optional_text(row, "Text"),
        optional_integer(row, "Score"),
        optional_text(row, "CreationDate"),
        optional_text(row, "ContentLicense"),
    )


def tag_record(row: Row) -> TagRecord:
    """Decode a tag row and its optional documentation references."""
    return TagRecord(
        integer(row, "Id"),
        text(row, "TagName"),
        optional_integer(row, "Count"),
        optional_integer(row, "WikiPostId"),
        optional_integer(row, "ExcerptPostId"),
    )
