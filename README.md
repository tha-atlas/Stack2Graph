# Stack2Graph

Stack2Graph imports Stack Overflow dumps into PostgreSQL and builds two search
outputs: a Qdrant vector collection and an RDF knowledge graph served by QLever.
Run SQL first to publish the source snapshot. Vector and Graph can then use that
snapshot independently, in either order.

## Getting started

Use Linux and run the examples from the repository root. Install Python 3.14,
UV, Docker Engine with Compose, and native 7-Zip 26.04 or newer (`7zz`).
Vector embedding requires NVIDIA CUDA GPUs. Graph indexing requires the QLever
CLI on the host. Torrent downloads require `aria2c`; these examples use HTTP.

### 1. Install the packages

```bash
uv sync --python python3.14 --no-python-downloads
```

For Vector, also install the embedding packages:

```bash
uv sync --extra embeddings
```

### 2. Configure the application and servers

Copy the examples when setting up a new installation. If `.env` files already
exist, edit them to preserve their settings.

```bash
cp .env.example .env
cp deployment/postgres/.env.example deployment/postgres/.env
cp deployment/qdrant/.env.example deployment/qdrant/.env
cp deployment/qlever/.env.example deployment/qlever/.env
```

Set application connections and processing options in the root `.env`. Replace
the example paths and configure the workflows you plan to run:

| Root `.env` entries | What to set |
| --- | --- |
| `STACK2GRAPH_DATA_ROOT`, `STACK2GRAPH_DATA_DIR`, `STACK2GRAPH_DATA_XML_PATH`, `STACK2GRAPH_DATA_POSTGRES_PATH` | Absolute paths outside this repository. Update all explicitly configured paths, including Vector cache, run and report directories. |
| `STACK2GRAPH_POSTGRES_HOST`, `STACK2GRAPH_POSTGRES_PORT`, `STACK2GRAPH_POSTGRES_USER`, `STACK2GRAPH_POSTGRES_DATABASE`, `STACK2GRAPH_POSTGRES_PASSWORD_FILE` | PostgreSQL client connection and the absolute path to a file containing its password. |
| `STACK2GRAPH_VECTOR_SNAPSHOT`, `STACK2GRAPH_GRAPH_SNAPSHOT` | The published SQL snapshot item ID, represented below by `SNAPSHOT_ID`. Enable the commented Graph entry. Set both to the same ID when validating across workflows. |
| `STACK2GRAPH_VECTOR_QDRANT_HOST`, `STACK2GRAPH_VECTOR_QDRANT_HTTP_PORT`, `STACK2GRAPH_VECTOR_QDRANT_GRPC_PORT`, `STACK2GRAPH_VECTOR_QDRANT_TLS`, `STACK2GRAPH_VECTOR_QDRANT_API_KEY` | For local Qdrant: `127.0.0.1`, ports `6333`/`6334`, TLS `false`, and the server's API key. |
| `STACK2GRAPH_VECTOR_COLLECTION`, `STACK2GRAPH_VECTOR_DENSE_REVISION`, `STACK2GRAPH_VECTOR_SPARSE_REVISION` | Collection name and immutable 40-character Hub commit SHAs for Qwen3-Embedding-8B and BGE-M3. |
| `STACK2GRAPH_VECTOR_GPUS` | Visible CUDA device ordinals. Use `0` for one GPU; the example file selects eight. |
| `STACK2GRAPH_QLEVER_EXECUTABLE`, `STACK2GRAPH_QLEVER_HOST` | Host QLever CLI path, if it is not on `PATH`, and service address, normally `127.0.0.1`. |

Each server reads its own deployment `.env`. Set PostgreSQL storage, listener,
user, database, and password file in `deployment/postgres/.env`, and use matching
credentials in the application. Set Qdrant storage, listener ports, and API key in
`deployment/qdrant/.env`. Set the QLever image digest, index and deployment paths,
server port, and query-memory budget in `deployment/qlever/.env`.

Create the external storage directories and PostgreSQL password file before
starting the services. The [PostgreSQL](deployment/postgres/README.md),
[Qdrant](deployment/qdrant/README.md) and [QLever](deployment/qlever/README.md)
setup instructions cover the required directories and permissions. Application
commands load root `.env` automatically.

### 3. Start the deployments

Start the databases as a Linux user with Docker access:

```bash
./deployment/postgres/run.sh up -d --wait
./deployment/qdrant/run.sh up -d --wait
./deployment/qlever/run.sh up -d --wait
```

SQL needs PostgreSQL. Vector also needs Qdrant, and a managed Graph build needs
QLever. Start the services your workflow uses. QLever serves an empty graph until
you publish the first build.

For public HTTPS, configure the optional gateway's hostname, certificates and
backend listener ports, then start it:

```bash
cp deployment/gateway/.env.example deployment/gateway/.env
# Configure the copied file before starting:
./deployment/gateway/run.sh up -d --wait
```

See the [gateway setup](deployment/gateway/README.md). Clients using the gateway
need its public endpoint and TLS settings; their ports can differ from the
backend listener ports.

### 4. Run SQL first

Replace `SNAPSHOT_ID` with the source's Internet Archive item ID:

```bash
uv run python -m sql.start run --snapshot SNAPSHOT_ID --download-method http
```

SQL downloads and verifies the archive, extracts the XML, imports the rows, and
publishes the PostgreSQL snapshot after validation. Use `latest` to discover a
release, then put the resolved item ID in the Vector and Graph settings. Check
local progress with:

```bash
uv run python -m sql.start status --json
```

### 5. Run Vector, Graph, or both

After SQL publishes the snapshot, you can run Vector, Graph, or both. Each reads
the published SQL data directly.

```bash
# Vector database:
uv run --extra embeddings python -m vector.start all --run first-build

# Knowledge graph:
uv run python -m graph.start build --run kg-full
```

Vector chunks question text, computes embeddings, uploads points, and finalizes
the collection. Graph exports RDF and builds, verifies, and publishes a QLever
index. The workflow guides explain how to resume interrupted jobs or rebuild
existing output.

### 6. Validate the results

When both builds finish, validate their completed runs against the same SQL
snapshot:

```bash
uv run python -m validation.start run --snapshot SNAPSHOT_ID --vector-run first-build --graph-run kg-full --run first-check
```

Use an individual check when only one output exists or when checking SQL before
a build:

```bash
uv run python -m validation.start postgres --snapshot SNAPSHOT_ID
uv run python -m validation.start vector --vector-run first-build
uv run python -m validation.start graph --graph-run kg-full
```

If you omit run selectors, validation selects completed runs for the snapshot
being checked. The combined `run` command requires both Vector and Graph output.

| Check | What it compares |
| --- | --- |
| `xml-postgres` | Recorded XML parser row counts against exact PostgreSQL counts for Posts, Comments, Tags, Votes and PostLinks. |
| `postgres-qdrant` | The Vector run's scanned-question count against its PostgreSQL scan window; Qdrant collection/build identity, dense dimension and exact point count against the finalized run. |
| `graph-postgres` | The Graph export's processed counts against PostgreSQL counts using the same export filters. |
| `graph-qdrant` | Successful validation against the same target, matching language registries, selected Vector questions within the Graph question count, and Qdrant collection/count agreement. |

Validation checks counts and identities. It does not measure search quality,
compare individual records, or confirm graph membership for every Vector
question. Graph builds run separate QLever count queries before publication.
Standalone validation compares export metadata with PostgreSQL without repeating
those queries. It reads existing data and writes a report; it leaves the datasets
unchanged.

A successful run prints a summary like this, with the actual timing and path:

```text
validation first-check passed in 12.3s (postgres=passed, vector=passed, graph=passed, qdrant=passed); report: first-check/report.json
```

The report is stored at
`<data_root>/validation-reports/first-check/report.json`, or under
`STACK2GRAPH_VALIDATION_RUN_DIR` if configured. It records the pinned target,
per-system and per-stage outcomes, timings and comparisons. Outcomes can be
`passed`, `failed` or `skipped`. The command exits with code `0` on success and `1`
on failure.

A count mismatch appears in the report as a comparison. This example shows two
missing Qdrant points:

```json
{
  "check": "qdrant.points",
  "outcome": "failed",
  "reason": "count-mismatch",
  "system": "qdrant",
  "expected": 12000,
  "actual": 11998,
  "difference": -2,
  "unit": "points"
}
```

## Workflow guides

| Area | Short guide | Full procedure |
| --- | --- | --- |
| SQL | [README](src/sql/README.md) | [Source verification, import, publication and recovery](doc/sql.md) |
| Vector | [README](src/vector/README.md) | [Selection, chunking, embeddings, loading and recovery](doc/vector.md) |
| Graph | [README](src/graph/README.md) | [RDF mapping, indexing, acceptance and rollback](doc/graph.md) |
