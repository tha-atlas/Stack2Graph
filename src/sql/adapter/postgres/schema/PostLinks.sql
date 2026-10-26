-- PostgreSQL 18 dump table; select the target schema with SET search_path first.
CREATE TABLE "PostLinks"
(
    "Id"              BIGINT NOT NULL,
    "PostId"          BIGINT,
    "RelatedPostId"   BIGINT,
    "LinkTypeId"      BIGINT,
    "CreationDate"    TEXT,
    "ExtraAttributes" JSONB
);
