"""Define five dump schemas and retain unknown attributes as JSON."""

import re
from dataclasses import dataclass
from datetime import date

SCHEMA_VERSION = "postgres-1"
IMPORT_VERSION = "copy-1"
TIMESTAMP_PATTERN = re.compile(r"(\d{4})-(\d{2})-(\d{2})(?:T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:Z)?)?")
EPOCH_DATE = date(1970, 1, 1)


@dataclass(frozen=True)
class Column:
    name: str
    kind: str
    nullable: bool = True


@dataclass(frozen=True)
class TableSpec:
    name: str
    xml_root: str
    columns: tuple[Column, ...]
    primary_id: str = "Id"


def _spec(name: str, integers: str, strings: str = "", dates: str = "", booleans: str = "") -> TableSpec:
    """Build a table schema with typed columns and original extra attributes."""
    columns = [Column("Id", "BIGINT", False)]
    for names, kind in ((integers, "BIGINT"), (strings, "VARCHAR"), (dates, "TIMESTAMP_TEXT"), (booleans, "BOOLEAN")):
        columns.extend(Column(field, kind) for field in names.split())
    columns.append(Column("ExtraAttributes", "JSON"))
    return TableSpec(name, name.lower(), tuple(columns))


TABLES = {
    spec.name: spec
    for spec in (
        _spec(
            "Posts",
            "PostTypeId AcceptedAnswerId ParentId Score ViewCount OwnerUserId LastEditorUserId "
            "AnswerCount CommentCount FavoriteCount",
            "Body OwnerDisplayName LastEditorDisplayName Title Tags ContentLicense",
            "CreationDate LastEditDate LastActivityDate ClosedDate CommunityOwnedDate",
        ),
        _spec("Comments", "PostId Score UserId", "Text UserDisplayName ContentLicense", "CreationDate"),
        _spec("PostLinks", "PostId RelatedPostId LinkTypeId", dates="CreationDate"),
        _spec("Tags", "Count ExcerptPostId WikiPostId", "TagName", booleans="IsModeratorOnly IsRequired"),
        _spec("Votes", "PostId VoteTypeId UserId BountyAmount", dates="CreationDate"),
    )
}


def timestamp_ns(value: str) -> int:
    """Convert an ISO UTC dump timestamp to epoch nanoseconds without floats."""
    match = TIMESTAMP_PATTERN.fullmatch(value)
    if not match:
        raise ValueError("Expected an ISO date or UTC timestamp with at most nine fractional digits")
    year, month, day, hour, minute, second, fraction = match.groups(default="")
    days = (date(int(year), int(month), int(day)) - EPOCH_DATE).days
    h, m, s = int(hour or 0), int(minute or 0), int(second or 0)
    if h > 23 or m > 59 or s > 59:
        raise ValueError("Invalid timestamp clock")
    result = (days * 86400 + h * 3600 + m * 60 + s) * 10**9 + int((fraction or "").ljust(9, "0"))
    return result
