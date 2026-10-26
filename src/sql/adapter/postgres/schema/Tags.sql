-- PostgreSQL 18 dump table; select the target schema with SET search_path first.
CREATE TABLE "Tags"
(
    "Id"              BIGINT NOT NULL,
    "Count"           BIGINT,
    "ExcerptPostId"   BIGINT,
    "WikiPostId"      BIGINT,
    "TagName"         TEXT,
    "IsModeratorOnly" BOOLEAN,
    "IsRequired"      BOOLEAN,
    "ExtraAttributes" JSONB
);
