# QLever service

Start and manage QLever through `run.sh`, which calls Docker Compose with
`compose.yml`. Service commands need Docker, but no host Python, UV, or QLever
CLI. The container prepares the first empty index and starts the native server.

Copy `deployment/qlever/.env.example` to `deployment/qlever/.env`. Set an
immutable image digest and external index and deployment directories. The
launcher reads the service `.env` independently of application configuration;
exported environment variables take precedence. Create both directories as the
non-root host user before starting the service.

Start QLever before building the graph:

```sh
./deployment/qlever/run.sh up -d --wait
```

The first start creates an empty native index and serves it through QLever.
It requires no PostgreSQL connection or prebuilt graph, and `ASK { ?s ?p ?o }`
returns false. Later starts restore the last successfully loaded graph. Indexes
and deployment records stay under the configured external roots. Script and
configuration paths resolve independently of the caller's working directory.

Build and publish the first graph:

```sh
uv run python -m graph.start build --run kg-full
```

Set `STACK2GRAPH_GRAPH_SNAPSHOT` in root `.env` to the published PostgreSQL
snapshot. The build exports RDF, creates a native index, tests it on a separate
query endpoint, and publishes it. It includes every exported language graph plus
schema and extra-languages, and chooses index paths automatically. The managed
command needs no `--candidate` or manual activation.

The service keeps serving its previous graph during export, indexing, and
verification. Publication briefly restarts it. A failure before publication
leaves the serving graph unchanged; replacement startup failure attempts to
restore the previous graph.

Resume an interrupted build using the same run name:

```sh
uv run python -m graph.start build --run kg-full --resume
```

Resume checks completed export shards against their recorded markers and
checksums. It reuses and reverifies completed indexes. Interrupted native
indexing starts again from the completed N-Quads export. Under the build lock,
the application removes its abandoned index directories and indexer container.
The service must be running before a build; stopping it during the build blocks
publication.

Keep `STACK2GRAPH_QLEVER_EXECUTABLE`, `STACK2GRAPH_QLEVER_HOST`,
`STACK2GRAPH_QLEVER_INDEX_RAM_BYTES` and `STACK2GRAPH_QLEVER_INDEX_TIMEOUT_SECONDS`
in root `.env` for the host CLI, client address and build budget. The index timeout
controls the host-side native index build watchdog and defaults to 24 hours.
`STACK2GRAPH_QLEVER_TIMEOUT_SECONDS` in the service file
continues to control server queries, readiness and service operations; increasing
the index limit therefore does not make ordinary requests wait longer.

The native indexer consumes the published shard catalog without a second pass
over all N-Quads for content hashing. QLever parses N-Quads in parallel and chooses
parser concurrency from the container's available resources. This deployment
sets no CPU limit.

Lifecycle commands:

```sh
./deployment/qlever/run.sh ps
./deployment/qlever/run.sh down
./deployment/qlever/run.sh up -d --wait
./deployment/qlever/run.sh logs --tail 100
uv run python -m graph.start rollback
```

`ps`, `logs`, `up`, `down`, `restart`, and other lifecycle operations are ordinary
Compose commands. `-d` starts in the background; `--wait` waits for the native
SPARQL healthcheck. `--wait-timeout SECONDS` sets the Compose startup deadline.
`down` preserves all external index files and the published index selection.
The graph pipeline switches verified indexes by recreating the Compose service;
rollback selects the preceding verified graph through the graph application.
A separately created candidate can be activated after successful verification:

```sh
uv run python -m graph.start verify --candidate /path/to/graph-indexes/candidate
uv run python -m graph.start publish --candidate /path/to/graph-indexes/candidate
```

An explicit configuration file replaces the default service file for the launcher.
For graph commands it replaces the application file and automatic service import.
A combined isolated file can configure both commands. As with the PostgreSQL and
Qdrant launchers, Compose options precede the command:

```sh
./deployment/qlever/run.sh --env-file /path/to/isolated.env up -d --wait
uv run python -m graph.start rollback --env-file /path/to/isolated.env
```

Keep deployment settings consistent for both commands. The graph application also
reads the root application `.env` and imports only `STACK2GRAPH_QLEVER_*` settings
from the service `.env` by default; unrelated service storage and database settings
cannot replace application settings. It still needs Python 3.14,
the project dependencies and the host QLever CLI for graph builds and verification.

## Public HTTPS

QLever HTTP stays bound to `127.0.0.1`. The independent
[gateway deployment](../gateway/README.md) provides HTTPS for QLever and Qdrant
without coupling either backend's lifecycle to the proxy.

Both `/qlever` and `/qlever/` reach the native SPARQL API. The gateway removes the
prefix and preserves GET query arguments and POST bodies. It exposes the API
without a separate browser UI. Configure external DNS, a matching certificate,
and network reachability before using it:

```sh
curl --fail --silent --show-error https://kg.example.com/qlever \
  --data-urlencode 'query=ASK {}' \
  --header 'Accept: application/sparql-results+json'
```

Use `--cacert /path/to/ca.pem` for a private CA; do not disable verification. The
gateway stays healthy independently of QLever. Requests return an upstream error
while QLever is stopped or switching indexes. Candidate verification containers
never receive public traffic.

## Independent artifact workflows

Use export and index separately for offline processing or portable artifacts.
The managed `build` command runs those same operations in sequence. Each index
records its source, schema, runtime, checksums, and query acceptance results.
The initial empty index records its runtime image without claiming a source
snapshot. Verification uses a separate loopback port and leaves the serving
index unchanged. The application controls containers only after checking their
ownership; it does not stop arbitrary processes by port.

References: [QLever](https://github.com/ad-freiburg/qlever),
[Docker Compose up](https://docs.docker.com/reference/cli/docker/compose/up/),
[Nginx proxy_pass](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_pass).

An optional `STACK2GRAPH_QLEVER_CONTAINER_NAME=stack2graph-qlever` fixes the
container name. Otherwise Compose derives it from the project name. The name
stays unchanged when the empty index is replaced by a graph or rolled back.
Compose ownership, runtime image and storage mounts are checked before the graph
pipeline controls an existing container. Verification containers keep separate
temporary names. Isolated deployments
should set a unique `STACK2GRAPH_QLEVER_COMPOSE_PROJECT` and omit the container-name
override, or supply a unique name beginning with `stack2graph-`.
