# Shared HTTPS gateway

The gateway provides HTTPS for Qdrant and QLever. Host networking lets it reach
their loopback listeners without joining either Compose project. It starts
independently of both backends; requests fail only when their target backend is
unavailable.

Copy `.env.example` to `.env`. Set the public DNS name covered by the certificate
and the absolute certificate paths, then start the gateway:

```sh
./deployment/gateway/run.sh config --quiet
./deployment/gateway/run.sh up -d --wait
```

The public routes are:

- `/qdrant/` for explicitly prefixed Qdrant REST and dashboard traffic
- native root paths such as `/collections` for existing Qdrant clients
- native `/qdrant.Service/Method` paths for Qdrant gRPC
- `/qlever/` for the QLever SPARQL API
- `/gateway/health` for gateway health independent of either backend

Clients send their usual Qdrant API key, and Nginx forwards it unchanged. The
gateway stores no copy. QLever currently has no application authentication.
This deployment holds the TLS configuration and private keys.

Bind the Qdrant and QLever HTTP ports to `127.0.0.1`. The launcher reads
`deployment/gateway/.env` only. Match `STACK2GRAPH_GATEWAY_QDRANT_PORT`,
`STACK2GRAPH_GATEWAY_QDRANT_GRPC_PORT`, and `STACK2GRAPH_GATEWAY_QLEVER_PORT` to the
backend listeners in their service files; defaults are 6333, 6334, and 7015.
Client endpoints in the root `.env` do not control routing. On Linux,
`network_mode: host` gives Nginx direct access to host loopback addresses.

For an external vector pipeline client, configure the public DNS name, port 443
and TLS while retaining the Qdrant API key:

```dotenv
STACK2GRAPH_VECTOR_QDRANT_HOST=services.example.com
STACK2GRAPH_VECTOR_QDRANT_HTTP_PORT=443
STACK2GRAPH_VECTOR_QDRANT_GRPC_PORT=443
STACK2GRAPH_VECTOR_QDRANT_TLS=true
```

Local pipelines can use `127.0.0.1`, ports 6333/6334, and
`STACK2GRAPH_VECTOR_QDRANT_TLS=false`. An unavailable HTTPS target does not trigger
a fallback to unencrypted HTTP.
