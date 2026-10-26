-- PostgreSQL 18 dump table; select the target schema with SET search_path first.
CREATE TABLE "Comments"
(
    "Id"              BIGINT NOT NULL,
    "PostId"          BIGINT,
    "Score"           BIGINT,
    "UserId"          BIGINT,
    "Text"            TEXT,
    "UserDisplayName" TEXT,
    "ContentLicense"  TEXT,
    "CreationDate"    TEXT,
    "ExtraAttributes" JSONB
);
