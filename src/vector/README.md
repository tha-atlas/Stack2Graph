# Vector

Vector creates a Qdrant collection from published PostgreSQL questions selected
by programming-language tags. It converts their HTML to text and splits that text
into chunks. Each chunk gets a dense Qwen3 embedding and sparse BGE-M3 weights.
Qdrant stores the source IDs and metadata needed to find the original question.

Run the stages `extract`, `embed`, `load`, and `finalize` separately, or use `all`
to overlap extraction, embedding, and upload as shards become ready. Finalization
waits for Qdrant indexing and verifies the point count before marking the run
complete.

## Usage

Run from the repository root after installing the embedding dependencies and
configuring the source snapshot, pinned model revisions and Qdrant connection.

```bash
uv run --extra embeddings python -m vector.start all --run first-build
```

Resume an interrupted run with the individual stage commands. Repeating `all`
requires `--overwrite` for existing local output. Replacing an existing Qdrant
collection also requires typing its exact name in an interactive terminal.

Read the [full Vector procedure](../../doc/vector.md) for selection, chunking,
models, configuration and recovery. Server setup is documented in the
[Qdrant deployment](../../deployment/qdrant/README.md); public HTTPS uses the
[optional gateway](../../deployment/gateway/README.md).
