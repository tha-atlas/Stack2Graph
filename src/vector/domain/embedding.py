"""Define embedding batch limits and GPU memory errors."""

from dataclasses import dataclass


class BatchTooLargeError(Exception):
    """Signal that a batch exceeds GPU memory and must be split by the caller."""


@dataclass(frozen=True)
class DenseBatching:
    batch_tokens: int
    batch_rows: int
    dimension: int


@dataclass(frozen=True)
class EmbedSettings:
    gpus: tuple[int, ...]
    dense_batch_tokens: int
    dense_batch_rows: int
    sparse_batch_size: int
    max_tokens: int
    dense_dimension: int
