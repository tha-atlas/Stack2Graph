"""Load literal vector settings and keep secrets out of configuration displays."""

from collections.abc import Mapping
from pathlib import Path

from selection.config import registry_path
from sql.config import load_postgres_config
from utils.config import load_data_paths, load_environment, outside_repository, run_root, validate_item_id
from vector.domain.config import ChunkConfig, InferenceConfig, PipelineLimits, QdrantConfig, RunConfig, StageConfig

PREFIX = "STACK2GRAPH_VECTOR_"


def load_qdrant_config(values: Mapping[str, str]) -> QdrantConfig:
    """Read collection connection settings and indexing limits."""

    def setting(name: str, default: str = "") -> str:
        """Read a prefixed vector setting or its default."""
        return values.get(PREFIX + name, default)

    tls = setting("QDRANT_TLS", "false").lower()
    if tls not in ("true", "false"):
        raise ValueError("QDRANT_TLS must be true or false")
    return QdrantConfig(
        setting("QDRANT_HOST"),
        setting("COLLECTION"),
        int(setting("QDRANT_HTTP_PORT", "6333")),
        int(setting("QDRANT_GRPC_PORT", "6334")),
        tls == "true",
        setting("QDRANT_API_KEY") or None,
        int(setting("REQUEST_TIMEOUT_SECONDS", "60")),
        int(setting("FINALIZE_TIMEOUT_SECONDS", "3600")),
        int(setting("INDEXING_THRESHOLD_KB", "10000")),
        int(setting("SHARD_NUMBER", "4")),
    )


def _gpu_list(text: str) -> tuple[int, ...]:
    """Parse comma-separated CUDA device ordinals."""
    try:
        return tuple(int(part) for part in text.split(",") if part.strip())
    except ValueError:
        raise ValueError("GPU lists must be comma-separated CUDA ordinals") from None


def load_run_config(environ: Mapping[str, str], env_file: Path | None = None) -> RunConfig:
    """Load vector settings, model revisions, and capped resource budgets."""
    from utils.resources import cap_budget, detect_limits, resource_budget

    values = load_environment(environ, env_file)

    def setting(name: str, default: str = "") -> str:
        """Read a prefixed vector setting or its default."""
        return values.get(PREFIX + name, default)

    def number(name: str, default: int) -> int:
        """Read an integer vector setting or its default."""
        return int(setting(name, str(default)))

    paths = load_data_paths({}, values)
    snapshot = setting("SNAPSHOT")
    validate_item_id(snapshot)
    detected = detect_limits()
    budget = resource_budget(
        detected,
        threads=int(setting("CPU_WORKERS")) if setting("CPU_WORKERS") else None,
        memory_bytes=int(setting("RAM_BYTES")) if setting("RAM_BYTES") else None,
    )
    return RunConfig(
        load_postgres_config(values),
        paths,
        snapshot,
        load_qdrant_config(values),
        ChunkConfig(number("CHUNK_TOKENS", 1024), number("CHUNK_OVERLAP", 128)),
        InferenceConfig(
            setting("DENSE_REVISION"),
            setting("SPARSE_REVISION"),
            outside_repository(setting("MODEL_CACHE_DIR", str(paths.data_root / "models"))),
        ),
        PipelineLimits(
            number("READ_BATCH_ROWS", 1024),
            number("READ_BATCH_BYTES", 16 * 1024**2),
            budget.threads,
            budget.memory_bytes,
            number("RETRY_ATTEMPTS", 3),
        ),
        registry_path(values),
        outside_repository(setting("REPORT_DIR", str(paths.data_root / "vector-reports"))),
        stages=StageConfig(
            run_root(values, paths.data_root, "VECTOR"),
            number("SHARD_CHILDREN", 8192),
            _gpu_list(setting("GPUS", "0,1,2,3,4,5,6,7")),
            number("DENSE_BATCH_TOKENS", 65536),
            number("DENSE_BATCH_ROWS", 256),
            number("SPARSE_BATCH_SIZE", 64),
            cap_budget(budget, threads=number("EXTRACT_WORKERS", 32)).threads,
            cap_budget(budget, threads=number("UPLOAD_WORKERS", 4)).threads,
            number("UPLOAD_BATCH_POINTS", 256),
            cap_budget(budget, threads=number("LOAD_PROCESSES", 8)).threads,
        ),
    )
