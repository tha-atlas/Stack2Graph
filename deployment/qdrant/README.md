# Qdrant

Qdrant stores dense and sparse vectors for search. Run these commands from the
repository root.

## Setup

For a new setup, copy [`.env.example`](.env.example) to this directory's `.env`.
Set server storage, listener ports, API key, Docker image, and Compose project
here. The [main `.env`](../../.env.example) configures the application endpoint
separately and must use the same API key. Through the HTTPS gateway, client ports
may differ from backend listener ports. Run the service as your normal Linux user
with Docker access, without `sudo`.

Replace the example path with your `STACK2GRAPH_DATA_ROOT` value, then create the
folders for stored data and saved snapshots:

```sh
vector_data_root=/absolute/path/to/stack2graph-data
mkdir -p -- "$vector_data_root/qdrant/storage" "$vector_data_root/qdrant/snapshots"
```

Keep Qdrant data outside the repository, preferably on SSD or NVMe storage.
Saved snapshots are local copies. This deployment does not schedule backups.

## Usage

```sh
./deployment/qdrant/run.sh config --quiet
./deployment/qdrant/run.sh up -d --wait
./deployment/qdrant/run.sh ps
./deployment/qdrant/run.sh --help
```

The commands validate configuration, start Qdrant, show service status, and print
help. Use `config --quiet` to keep the API key out of the output. The default
listener accepts connections from this machine.

The launcher reads this directory's `.env` independently of application
configuration. Place `--env-file FILE` before the command to use another complete
configuration. Relative paths resolve from the current working directory. Later
files override earlier files; shell variables override file values.

Service readiness confirms that Qdrant can accept requests. The vector build's
[finalize step](../../doc/vector.md#stages) waits for indexing to finish and checks
the completed collection.

## Public HTTPS

Qdrant listens on loopback HTTP and requires the configured API key. The
[gateway deployment](../gateway/README.md) exposes Qdrant and QLever over public
HTTPS. It forwards the client's Qdrant API key unchanged without storing a copy.

Local vector jobs can use HTTP at `127.0.0.1`, ports 6333/6334. External jobs use
the gateway's public DNS name, port 443, and TLS. Set the transport explicitly in
the application configuration. HTTPS failures do not trigger an HTTP fallback.

The service `.env` sets `STACK2GRAPH_VECTOR_QDRANT_CONTAINER_NAME=stack2graph-qdrant`.
For an isolated deployment, choose a unique name or omit the override so Compose
derives the name from its project and avoids collisions with production.
