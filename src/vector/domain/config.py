"""Validate vector settings separately from environment and resource loading."""

import re
from dataclasses import dataclass, field
from pathlib import Path

from sql.domain.postgres import PostgreSQLConfig
from utils.config import DataPaths


@dataclass(frozen=True)
class ChunkConfig:
    max_tokens: int = 1024
    overlap: int = 128

    def __post_init__(self) -> None:
        """Require a positive token limit and a smaller nonnegative overlap."""
        if self.max_tokens <= 0 or not 0 <= self.overlap < self.max_tokens:
            raise ValueError("Chunk tokens must be positive and greater than nonnegative overlap")


@dataclass(frozen=True)
class QdrantConfig:
    host: str
    collection: str
    http_port: int = 6333
    grpc_port: int = 6334
    tls: bool = False
    api_key: str | None = field(default=None, repr=False)
    request_timeout: int = 60
    finalize_timeout: int = 3600
    indexing_threshold_kb: int = 10000
    shard_number: int = 4

    def __post_init__(self) -> None:
        """Validate connection fields and positive indexing and timeout limits."""
        if not self.host or "/" in self.host or "@" in self.host:
            raise ValueError("Qdrant host must be a hostname or IP without URL or credentials")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", self.collection):
            raise ValueError("Qdrant collection must use letters, digits, underscores or hyphens")
        if not all(0 < port < 65536 for port in (self.http_port, self.grpc_port)):
            raise ValueError("Qdrant ports must be between 1 and 65535")
        if min(self.request_timeout, self.finalize_timeout) <= 0:
            raise ValueError("Qdrant deadlines must be positive")
        if min(self.indexing_threshold_kb, self.shard_number) <= 0:
            raise ValueError("Qdrant indexing threshold and shard count must be positive")

    @property
    def endpoint(self) -> str:
        """The HTTP endpoint using the configured TLS setting and port."""
        return f"{'https' if self.tls else 'http'}://{self.host}:{self.http_port}"


@dataclass(frozen=True)
class InferenceConfig:
    dense_revision: str
    sparse_revision: str
    cache_dir: Path

    def __post_init__(self) -> None:
        """Require immutable 40-character commit hashes for both models."""
        if not all(re.fullmatch(r"[0-9a-f]{40}", revision) for revision in (self.dense_revision, self.sparse_revision)):
            raise ValueError("Both model revisions must be immutable 40-character commit SHAs")


@dataclass(frozen=True)
class PipelineLimits:
    """Set shared source-read budgets and retry limits for vector stages."""

    read_rows: int = 1024
    read_bytes: int = 16 * 1024**2
    cpu_workers: int = 1
    ram_bytes: int = 128 * 1024**2
    attempts: int = 3

    def __post_init__(self) -> None:
        """Validate positive budgets, retry limits, and the read-buffer bound."""
        if min(self.read_rows, self.read_bytes, self.cpu_workers, self.ram_bytes, self.attempts) <= 0:
            raise ValueError("All pipeline budgets must be positive")
        if self.attempts > 10:
            raise ValueError("Retry attempts must not exceed 10")
        if self.read_bytes > self.ram_bytes:
            raise ValueError("Read buffer exceeds the RAM budget")


@dataclass(frozen=True)
class StageConfig:
    """Set shard sizes, GPU assignments, and worker counts for a vector build."""

    run_dir: Path
    shard_children: int = 8192
    gpus: tuple[int, ...] = (0, 1, 2, 3, 4, 5, 6, 7)
    dense_batch_tokens: int = 65536
    dense_batch_rows: int = 256
    sparse_batch_size: int = 64
    extract_workers: int = 32
    upload_workers: int = 4
    upload_points: int = 256
    load_processes: int = 8

    def __post_init__(self) -> None:
        """Require positive stage budgets and distinct nonnegative GPU ordinals."""
        budgets = (
            self.shard_children,
            self.dense_batch_tokens,
            self.dense_batch_rows,
            self.sparse_batch_size,
            self.extract_workers,
            self.upload_workers,
            self.upload_points,
            self.load_processes,
        )
        if min(budgets) <= 0:
            raise ValueError("Stage budgets must be positive")
        if not self.gpus or len(set(self.gpus)) != len(self.gpus) or min(self.gpus) < 0:
            raise ValueError("GPUs must be a non-empty list of distinct nonnegative CUDA ordinals")


@dataclass(frozen=True)
class RunConfig:
    postgres: PostgreSQLConfig
    paths: DataPaths
    snapshot: str
    qdrant: QdrantConfig
    chunks: ChunkConfig
    inference: InferenceConfig
    limits: PipelineLimits
    registry_path: Path
    report_dir: Path
    stages: StageConfig
