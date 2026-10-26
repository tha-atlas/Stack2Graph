# Vector procedure

Vector indexes selected questions from a published PostgreSQL snapshot in
Qdrant. It splits each question into text chunks and stores a dense semantic
vector, sparse lexical weights, and source-identifying metadata for each chunk.

## Setup

First publish a snapshot with [SQL](sql.md). Install the optional embedding
packages in the Python 3.14 project environment:

```bash
uv sync --extra embeddings
```

Each NVIDIA CUDA GPU worker loads `Qwen/Qwen3-Embedding-8B` and `BAAI/bge-m3`.
Allow GPU memory for both models and their working buffers. The example selects
eight visible GPU ordinals. For one GPU, set `STACK2GRAPH_VECTOR_GPUS=0`.

Set the published snapshot item ID, PostgreSQL connection, Qdrant endpoint,
collection name, and model revisions in root `.env`. Pin each model to a
40-character Hub commit SHA. The first use downloads the model into the configured
cache; later calls reuse that revision.

The Qdrant server has a separate configuration. For a new local deployment:

```bash
cp deployment/qdrant/.env.example deployment/qdrant/.env
```

Create the server storage directories and configure its API key and listener
ports as described in the [Qdrant setup](../deployment/qdrant/README.md), then start
it:

```bash
./deployment/qdrant/run.sh up -d --wait
```

Use the same API key in the application and server. Local clients can connect
to loopback HTTP and gRPC ports. For the [HTTPS gateway](../deployment/gateway/README.md),
use its public host, ports, and TLS setting. Root `.env` client ports do not change
the server listeners.

All examples below run from the repository root. Application commands load root
`.env` automatically, with exported environment variables taking precedence.
An explicit `--env-file /absolute/path/to/alternative.env` selects a different
application file.

## Question selection

The reader opens a fixed published snapshot and streams questions in ID order.
The language registry matches base tags, reviewed synonyms, and version patterns.
A question can match several languages; its payload keeps every matching name.

Questions with no language match count as not selected. Score, accepted-answer
status, and closure status do not filter selection; they remain metadata.
Embeddings use question text only. Answers and comments remain in PostgreSQL
and the graph.

The packaged registry is the default. `STACK2GRAPH_SELECTION_REGISTRY_PATH`
selects an alternative reviewed registry. Its digest is part of the run identity,
so changing language selection requires a separate compatible build.

## Text preparation

Text preparation removes scripts and styles from the HTML body, adds line breaks
at block boundaries, and preserves whitespace inside code. Every model input
starts with the question title as `Title: ...`.

Each chunk must fit both model tokenizers. Defaults allow 1024 input tokens,
including the repeated title, with 128 tokens of overlap. Splits prefer line
boundaries. Overlap avoids repeating code spans. Chunk records keep the original
text offsets and overlap lengths.

If the title alone exceeds the token limit, or the text cannot be split within
the budget, extraction records a chunking error and skips the question. It treats
questions with no visible title or body the same way.

Extraction writes whole questions into chunk shards. A shard can exceed its target
child count when needed to keep all chunks of one question together.

## Stages

| Command | Work |
| --- | --- |
| `extract` | Read selected source questions, prepare chunks and commit chunk shards. |
| `embed` | Produce dense and sparse embeddings for completed shards. |
| `load` | Upload shards for which both embedding outputs are complete. |
| `finalize` | Finish indexing, verify readiness and counts, then complete the run. |
| `all` | Overlap extraction, embedding and loading, then finalize. |

For a new full build:

```bash
uv run --extra embeddings python -m vector.start all --run first-build
```

Before loading models, `all` checks Qdrant. Extraction publishes completed
shards for GPU workers to embed and loaders to upload while later questions are
still being prepared. If a stage fails, the other active stages stop.

Individual stages use the same run name:

```bash
uv run --extra embeddings python -m vector.start extract --run first-build
uv run --extra embeddings python -m vector.start embed --run first-build
uv run python -m vector.start load --run first-build
uv run python -m vector.start finalize --run first-build
```

`extract` needs the model tokenizers. `embed` performs GPU inference. Loading
completed embeddings and finalization do not run the models.

`--after-id` and `--limit-questions` bound the first extraction of a run. They are
useful for a smaller trial before a full build. The manifest retains that scan
scope for later stages.

## Models and Qdrant records

Qwen3 uses last-token pooling and L2 normalization to produce 4096-dimensional
vectors. Dense shards store float16 values. Batches have token and row limits.
A batch that exceeds GPU memory is split and retried; a single row that still
cannot fit fails the stage.

BGE-M3 supplies sparse lexical weights. Its dense and ColBERT outputs are disabled.
The sparse shard files store token indices, weights and row offsets.

Qdrant stores named vectors `dense` and `sparse`, with cosine distance for dense
similarity. Snapshot digest, configuration digest, question ID, and chunk index
determine each point ID. Uploading the same point again replaces it.

Payloads include the question ID, chunk index and count, original tags, matched
languages, score, answer counts, accepted-answer information, closed status and
source dates. The payload's `snapshot_id` is the published snapshot digest, while
`STACK2GRAPH_VECTOR_SNAPSHOT` selects the source by item ID. Source text stays in
local chunk files and PostgreSQL; it is not copied into the Qdrant payload.

Collection creation configures dense and sparse indexes and metadata filter
indexes. Bulk loading starts with dense indexing disabled. Finalization restores
the configured indexing threshold and waits for the collection to be ready.

## Validation

Completion markers record each chunk shard and its dense, sparse, and upload
stages. Finalization starts only after extraction finishes and every chunk shard
has an upload marker.

Qdrant must pass readiness checks and report the exact point count recorded by
the completed shards. The `postgres-qdrant` check compares source and run metadata
before writing `report.json` and `finalized.done`. A failed check leaves completed
stage files in place and the run unfinalized.

The report records point and question counts, skipped questions, chunking errors,
shard counts, stage timings and the worker settings used for the run. Since stages
overlap in `all`, their individual durations do not add up to elapsed build time.

## Configuration

The [root example](../.env.example) lists application settings. All names in the
following table have the `STACK2GRAPH_VECTOR_` prefix.

| Suffixes | Purpose |
| --- | --- |
| `SNAPSHOT` | Published SQL snapshot item ID. |
| `QDRANT_HOST`, `QDRANT_HTTP_PORT`, `QDRANT_GRPC_PORT`, `QDRANT_TLS`, `QDRANT_API_KEY` | Client connection and authentication. |
| `COLLECTION`, `SHARD_NUMBER`, `INDEXING_THRESHOLD_KB` | Collection identity and indexing configuration. |
| `DENSE_REVISION`, `SPARSE_REVISION`, `MODEL_CACHE_DIR` | Immutable model revisions and cached model files. |
| `RUN_DIR`, `REPORT_DIR` | Stage artifacts and execution logs. |
| `CHUNK_TOKENS`, `CHUNK_OVERLAP`, `SHARD_CHILDREN` | Text chunk limits and target shard size. |
| `GPUS`, `DENSE_BATCH_TOKENS`, `DENSE_BATCH_ROWS`, `SPARSE_BATCH_SIZE` | GPU selection and inference batches. |
| `EXTRACT_WORKERS`, `LOAD_PROCESSES`, `UPLOAD_WORKERS`, `UPLOAD_BATCH_POINTS` | CPU preparation and upload concurrency. |
| `READ_BATCH_ROWS`, `READ_BATCH_BYTES` | Bounded source reads. |
| `CPU_WORKERS`, `RAM_BYTES` | Optional upper limits on the detected CPU and RAM budget. |
| `REQUEST_TIMEOUT_SECONDS`, `FINALIZE_TIMEOUT_SECONDS`, `RETRY_ATTEMPTS` | Request deadlines, finalization deadline and transient-error retries. |

Detected CPU and memory budgets cap worker requests. The Qdrant image, container
name, physical storage and server listener configuration belong in
[`deployment/qdrant/.env`](../deployment/qdrant/.env.example).

## Artifacts and recovery

A run lives at `<vector_run_root>/<run>`, defaulting to
`<data_root>/vector-runs/<run>`. It contains:

- `manifest.json`, binding source, selection, model revisions and chunk settings;
- `chunks/`, with JSONL chunks, completion markers and extraction completion;
- `embeddings/`, with dense and sparse arrays, worker records and stage markers;
- `loaded/`, with upload markers;
- `report.json` and `finalized.done`, written after successful finalization.

Execution logs go to `STACK2GRAPH_VECTOR_REPORT_DIR`, defaulting to
`<data_root>/vector-reports`.

A run lock prevents competing invocations. Workers claim a shard, publish its
complete output atomically, and write a completion marker. Restart removes
partial writes and stale claims. Repeating a stage reuses compatible completed
files and processes unfinished work. Keep the source, registry, models, and
chunk settings unchanged when resuming.

Repeating `all` requires `--overwrite` to clear existing local stage files.
Replacing an existing collection requires a separate confirmation: type its exact
name in an interactive terminal. Unattended invocations cannot confirm deletion.

For a separate read-only count report on completed Vector output:

```bash
uv run python -m validation.start vector --vector-run first-build
```

Return to the [Vector overview](../src/vector/README.md), or continue with
[Graph](graph.md) when the application also needs relationships around the source
questions. A Graph build does not require the Vector collection.
