-- PostgreSQL 18 indexes matching PostgreSQLDumpStore._prepare_indexes().
-- Select the target schema with SET search_path before running this file.
-- The importer creates these automatically when publishing a snapshot.

CREATE UNIQUE INDEX IF NOT EXISTS "Posts_id" ON "Posts" ("Id");
CREATE UNIQUE INDEX IF NOT EXISTS "Comments_id" ON "Comments" ("Id");
CREATE UNIQUE INDEX IF NOT EXISTS "PostLinks_id" ON "PostLinks" ("Id");
CREATE UNIQUE INDEX IF NOT EXISTS "Tags_id" ON "Tags" ("Id");
CREATE UNIQUE INDEX IF NOT EXISTS "Votes_id" ON "Votes" ("Id");

CREATE INDEX IF NOT EXISTS "posts_type_id" ON "Posts" ("PostTypeId", "Id");
CREATE INDEX IF NOT EXISTS "posts_parent_type" ON "Posts" ("ParentId", "PostTypeId");
CREATE INDEX IF NOT EXISTS "comments_post" ON "Comments" ("PostId");
CREATE INDEX IF NOT EXISTS "tags_name" ON "Tags" ("TagName");
CREATE INDEX IF NOT EXISTS "votes_post_type" ON "Votes" ("PostId", "VoteTypeId");
CREATE INDEX IF NOT EXISTS "links_post" ON "PostLinks" ("PostId");
CREATE INDEX IF NOT EXISTS "links_related" ON "PostLinks" ("RelatedPostId");

ANALYZE "Posts";
ANALYZE "Comments";
ANALYZE "PostLinks";
ANALYZE "Tags";
ANALYZE "Votes";
