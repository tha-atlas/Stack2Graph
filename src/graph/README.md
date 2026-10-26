# Graph

Graph exports a published PostgreSQL snapshot as RDF named graphs. Each
programming-language graph contains its questions and their answers, comments,
and links. Unmatched questions go into `extra-languages`. The export also contains
tag documentation and the ontology in a separate schema graph.

A managed build writes N-Quads, creates a QLever index, and tests that index on
a separate query endpoint before publication. The service continues serving its
previous graph during the build and briefly restarts when the new index is
published.

## Usage

Configure the source snapshot, host QLever CLI, and deployment. From the
repository root, start QLever and then run the managed build.

```sh
./deployment/qlever/run.sh up -d --wait
uv run python -m graph.start build --run kg-full
```

An interrupted build can continue with the same run name:

```sh
uv run python -m graph.start build --run kg-full --resume
```

Read the [full Graph procedure](../../doc/graph.md) for RDF mapping, named graphs,
artifact workflows, configuration and rollback. Server setup is documented in the
[QLever deployment](../../deployment/qlever/README.md).
