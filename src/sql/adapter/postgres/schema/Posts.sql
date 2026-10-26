-- PostgreSQL 18 dump table; select the target schema with SET search_path first.
CREATE TABLE "Posts"
(
    "Id"                    BIGINT NOT NULL,
    "PostTypeId"            BIGINT,
    "AcceptedAnswerId"      BIGINT,
    "ParentId"              BIGINT,
    "Score"                 BIGINT,
    "ViewCount"             BIGINT,
    "OwnerUserId"           BIGINT,
    "LastEditorUserId"      BIGINT,
    "AnswerCount"           BIGINT,
    "CommentCount"          BIGINT,
    "FavoriteCount"         BIGINT,
    "Body"                  TEXT,
    "OwnerDisplayName"      TEXT,
    "LastEditorDisplayName" TEXT,
    "Title"                 TEXT,
    "Tags"                  TEXT,
    "ContentLicense"        TEXT,
    "CreationDate"          TEXT,
    "LastEditDate"          TEXT,
    "LastActivityDate"      TEXT,
    "ClosedDate"            TEXT,
    "CommunityOwnedDate"    TEXT,
    "ExtraAttributes"       JSONB
);
