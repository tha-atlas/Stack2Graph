-- PostgreSQL 18 dump table; select the target schema with SET search_path first.
CREATE TABLE "Votes"
(
    "Id"              BIGINT NOT NULL,
    "PostId"          BIGINT,
    "VoteTypeId"      BIGINT,
    "UserId"          BIGINT,
    "BountyAmount"    BIGINT,
    "CreationDate"    TEXT,
    "ExtraAttributes" JSONB
);
