# Graph procedure

Graph exports published PostgreSQL data as RDF named graphs and builds a QLever
index. A managed build tests the new index with SPARQL queries before switching
the service to it.

## Setup

Publish a snapshot with [SQL](sql.md) first. Install Python 3.14, the project
dependencies, and the host QLever CLI for native index builds and verification.
Docker Compose manages the serving database separately. Graph needs no Vector
embeddings or GPUs.

For a new setup:

```bash
cp deployment/qlever/.env.example deployment/qlever/.env
```

In root `.env`, set `STACK2GRAPH_GRAPH_SNAPSHOT` to the published source item ID and
configure the PostgreSQL connection. Set `STACK2GRAPH_QLEVER_EXECUTABLE` if the host
CLI is not available as `qlever` on `PATH`. `STACK2GRAPH_QLEVER_HOST` is the address
used to contact the service.

Configure the immutable QLever image digest, server port, index root, deployment
root and container identity in `deployment/qlever/.env`. Create the external
storage directories as the host user, following the
[QLever setup](../deployment/qlever/README.md).

Run all examples from the repository root. Application commands load root `.env`
automatically. Build, index, verification and publication commands also import
`STACK2GRAPH_QLEVER_*` values from the service file. Unrelated service settings do
not replace application paths or PostgreSQL credentials. Exported environment
variables take precedence.

An explicit `--env-file /absolute/path/to/alternative.env` replaces the application
file and automatic service import. Supply all settings needed by that command in
the alternative file. The service launcher normally loads only its own `.env`.

## Named graphs and source scope

Graph and Vector use the same language registry. Base tags, reviewed synonyms,
and version patterns determine graph membership. A question that matches several
languages appears with its related records in every matching language graph.

Questions without a language match are assigned to `extra-languages`. The `schema`
graph contains the ontology. Named graph IRIs use `http://stackoverflow.com/`
followed by the encoded language or reserved graph name, for example
`http://stackoverflow.com/python`.

Each graph contains questions and their answers, comments, tag references, and
supported outgoing question links. Shared records include tags, wikis, excerpts,
and their related content. Posts carry upvote and downvote counts. Type 1 links
map to `linkedTo`; type 3 links map to `duplicateOf`. Repeated source rows for one
relationship produce one logical edge.

## RDF mapping

Questions use IRIs such as `http://stackoverflow.com/q/123`; answers use
`http://stackoverflow.com/a/456`. Tags use percent-encoded names under
`http://stackoverflow.com/tags/`. The ontology namespace is
`http://stackoverflow.com/schema#`.

The mapping preserves titles, body HTML, scores, counts, dates, and content
licenses when available. Missing attributes stay absent rather than becoming
zero. Dates use typed RDF literals. Comment IRIs retain the source comment and
parent IDs.

Questions connect to answers, accepted answers, comments, and tags. Mapping
checks parent and accepted-answer references and rejects contradictory answer
counts. Source evidence determines whether a question is answered or unanswered.
Declared inverse properties produce edges in both directions, and type
statements follow the ontology.

The same source entity can occur in several language graphs. Use `DISTINCT` on
source IDs or entity IRIs when a cross-graph query needs unique entities. Raw quad
counts include those repeated graph memberships and are not source-row counts.

## Managed build

Start the service before a managed build:

```sh
./deployment/qlever/run.sh up -d --wait
uv run python -m graph.start build --run kg-full
```

The service starts with an empty native index until a graph has been published.
Later starts restore the last selected graph. A managed build performs these
stages:

1. Check the managed service and its deployment identity.
2. Export the selected PostgreSQL snapshot into N-Quads artifacts.
3. Select all exported language graphs together with the required reserved graphs
   and build a native QLever candidate.
4. Start a separate temporary query endpoint and verify the candidate.
5. Publish the verified index and recreate the serving container.

The service keeps its current graph available during export, indexing, and
verification. Publication briefly restarts it. Failures before publication leave
the serving graph unchanged. If switching fails, the application attempts to
restore the previous selection.

A managed build derives the candidate path from the completed export identity.
It can reuse and reverify an existing candidate when the source and export
identities match.

## Export and artifacts

Export workers process bounded question-ID ranges within limits for concurrency,
batch bytes, record size, temporary storage, and free disk space. N-Quads shards
rotate at the configured target size. Choose `none` or `gzip` for compression.

A run records the published snapshot digest, language registry, ontology,
mapping, exporter code, and output settings. Keep those inputs and dependency
versions unchanged throughout export and resume.

An export run lives at `<graph_run_root>/<run>`, defaulting to
`<data_root>/graph-runs/<run>`. It contains the build identity, range completion
records, shard catalogs and checksums, question membership records, shared and
schema output, and a final `manifest.json`. The manifest records processed source
counts for Posts, Comments, Tags, Votes and PostLinks.

Native index directories live under `STACK2GRAPH_QLEVER_INDEX_ROOT`. They include
index files, `index.json`, expected query counts and acceptance evidence. The
service selection lives under `STACK2GRAPH_QLEVER_DEPLOYMENT_ROOT`. Application
execution logs go to `<data_root>/graph-reports`.

## Acceptance and publication

For a complete graph selection, verification queries QLever for five logical
counts: Posts, Comments, Tags, Votes and PostLinks. Counts deduplicate entities
across named graphs. Vote counts sum each post's upvotes and downvotes once;
PostLinks counts use distinct supported relationships.

Expected counts record the source rows processed by the export, including only
supported vote and link types. They differ from unfiltered source-table counts.
Verification uses the saved counts without rescanning PostgreSQL or parsing the
full N-Quads export.

A count mismatch blocks acceptance. A manually selected language subset uses a
queryability check because full-export counts do not apply to that subset.
Managed builds include the complete export and run all five count checks.

Publication requires a verified candidate. Before controlling an existing
container, the application checks its Compose project, runtime image, and storage
mounts. Verification containers have separate names and loopback ports.

## Interrupted builds and rollback

Continue an interrupted managed build with the same run name:

```sh
uv run python -m graph.start build --run kg-full --resume
```

Resume verifies completed export files through their manifests, markers, and
checksums. Native indexing starts again from the completed export if the index
was interrupted. Cleanup removes abandoned build directories and indexer
containers belonging to that build while holding its lock. Publication requires
the serving service to be running.

To restore the preceding verified graph:

```sh
uv run python -m graph.start rollback
```

Rollback operates on the managed service selection. The deployment commands for
`ps`, `logs`, `restart` and `down` remain ordinary service lifecycle operations;
`down` preserves the external index files and selection records.

## Independent artifact workflows

Exporting RDF does not require a running QLever service:

```sh
uv run python -m graph.start export --run kg-export
uv run python -m graph.start resume --run kg-export
```

Use `resume` to continue that export with unchanged inputs. A completed manifest
is required before indexing its artifacts. Build and verify a manually selected
candidate as follows, replacing the paths with the actual external directories:

```sh
uv run python -m graph.start index --artifacts /absolute/path/to/graph-runs/kg-export --candidate /absolute/path/to/graph-indexes/candidate
uv run python -m graph.start verify --candidate /absolute/path/to/graph-indexes/candidate
uv run python -m graph.start publish --candidate /absolute/path/to/graph-indexes/candidate
```

`index --languages python java` limits the selected language graphs. The required
schema and extra-language artifacts remain part of the index selection. `stream`
reads an explicit shard catalog and writes its N-Quads to standard output:

```sh
uv run python -m graph.start stream --catalog /absolute/path/to/shard-catalog.jsonl
```

`benchmark` exports a bounded question range under several worker, batch and
compression settings and writes a report:

```sh
uv run python -m graph.start benchmark --first-id 1 --end-id 10000
```

## Configuration

All names in the following table have the `STACK2GRAPH_GRAPH_` prefix and belong
in root `.env`.

| Suffixes | Purpose |
| --- | --- |
| `SNAPSHOT`, `RUN_DIR` | Published source item ID and export run root. |
| `RANGE_WIDTH`, `SHARD_BYTES` | Question ID range width and target shard size. |
| `WORKERS`, `RAM_BYTES` | Upper limits on detected CPU and memory budgets. |
| `BATCH_ROWS`, `BATCH_BYTES`, `RECORD_BYTES` | Source batch and individual-record limits. |
| `SPILL_BYTES`, `DISK_RESERVE_BYTES` | Temporary-space budget and filesystem headroom. |
| `COMPRESSION`, `COMPRESSION_LEVEL` | `none` or `gzip`; a gzip level from 1 to 9. |
| `ARTIFACT_ROOT` | Default artifact location for the standalone `index` command; point it to a completed export run or pass `--artifacts`. |

The host build also uses `STACK2GRAPH_QLEVER_EXECUTABLE`, `STACK2GRAPH_QLEVER_HOST`,
`STACK2GRAPH_QLEVER_INDEX_RAM_BYTES` and
`STACK2GRAPH_QLEVER_INDEX_TIMEOUT_SECONDS` from root `.env`. The index timeout
defaults to 24 hours. Server query memory and request/service timeouts belong in
`deployment/qlever/.env`, alongside the image and storage settings. See the
[application example](../.env.example) and
[service example](../deployment/qlever/.env.example).

For a separate read-only comparison of completed export counts with PostgreSQL,
select the export run. This check does not repeat native QLever acceptance queries.

```sh
uv run python -m validation.start graph --graph-run kg-full
```

## Querying the graph

QLever serves SPARQL over its local HTTP endpoint. For example, this query returns
up to ten question titles in the Python graph:

```sparql
PREFIX so: <http://stackoverflow.com/schema#>

SELECT DISTINCT ?question ?title WHERE {
  GRAPH <http://stackoverflow.com/python> {
    ?question a so:Question ; so:hasTitle ?title .
  }
}
LIMIT 10
```

Follow `so:hasAnswer`, `so:acceptedAnswer`, and `so:hasComment` to retrieve related
content. Follow `so:linkedTo` and `so:duplicateOf` for question relationships.
A linked question's body may be stored in another language graph, so a query may
need to search several named graphs.

The optional
[HTTPS gateway](../deployment/gateway/README.md) exposes that endpoint under
`/qlever/`. Return to the [Graph overview](../src/graph/README.md) for the usual
managed-build commands.
