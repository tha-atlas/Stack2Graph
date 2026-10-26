"""Resolve dense and sparse model files at fixed revisions."""

from dataclasses import dataclass
from pathlib import Path

from vector.domain.config import InferenceConfig

DENSE_MODEL = "Qwen/Qwen3-Embedding-8B"
DENSE_DIMENSION = 4096
SPARSE_MODEL = "BAAI/bge-m3"
_IGNORED = ["*.onnx", "onnx/*", "*.h5", "*.msgpack"]


@dataclass(frozen=True)
class ModelPaths:
    dense: Path
    sparse: Path


def resolve_model_paths(config: InferenceConfig) -> ModelPaths:
    """Cache pinned model revisions once and resolve later calls locally."""
    from huggingface_hub import snapshot_download

    dense = snapshot_download(
        DENSE_MODEL, revision=config.dense_revision, cache_dir=str(config.cache_dir), ignore_patterns=_IGNORED
    )
    sparse = snapshot_download(
        SPARSE_MODEL, revision=config.sparse_revision, cache_dir=str(config.cache_dir), ignore_patterns=_IGNORED
    )
    return ModelPaths(Path(dense), Path(sparse))
