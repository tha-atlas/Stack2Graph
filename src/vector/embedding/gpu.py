"""Run CUDA encoders in supervised per-GPU worker processes."""

import multiprocessing as mp
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from multiprocessing.process import BaseProcess
from pathlib import Path
from threading import Event
from typing import Literal, Protocol, TypedDict

import numpy as np
import torch
from numpy.typing import NDArray

from utils.artifacts import atomic_write_json
from vector.domain.embedding import BatchTooLargeError, DenseBatching, EmbedSettings
from vector.domain.models import SparseWeights
from vector.embedding.embed import embed_dense_shard, embed_sparse_shard, gpu_worker_loop
from vector.embedding.model_files import ModelPaths
from vector.progress import configure_embedding_output
from vector.shards import RunLayout
from vector.workers import guarded_worker, parent_process_alive, supervise_processes


class _Tokenizer(Protocol):
    def __call__(
        self, text: list[str], *, padding: bool, truncation: bool, return_tensors: Literal["pt"]
    ) -> Mapping[str, torch.Tensor]:
        """Tokenize texts into tensors with the requested padding policy."""
        ...


class _TokenizerFactory(Protocol):
    def from_pretrained(
        self, pretrained_model_name_or_path: str, *, local_files_only: bool, padding_side: str
    ) -> _Tokenizer:
        """Load a tokenizer with the requested local-file and padding settings."""
        ...


class _Output(Protocol):
    @property
    def last_hidden_state(self) -> torch.Tensor:
        """The model's final hidden states for the input tokens."""
        ...


class _Model(Protocol):
    def __call__(self, *, input_ids: torch.Tensor, attention_mask: torch.Tensor) -> _Output:
        """Run the model for token IDs and their attention mask."""
        ...


class _ModelFactory(Protocol):
    def from_pretrained(
        self,
        pretrained_model_name_or_path: str,
        *,
        local_files_only: bool,
        dtype: torch.dtype,
        attn_implementation: str,
        device_map: str,
    ) -> _Model:
        """Load model weights with the requested dtype, attention, and device."""
        ...


class _HalfTensor(Protocol):
    def numpy(self) -> NDArray[np.float16]:
        """Return CPU tensor values as a float16 NumPy array."""
        ...


@dataclass(frozen=True)
class DenseInputs:
    input_ids: torch.Tensor
    attention_mask: torch.Tensor


class QwenDenseEncoder:
    """
    Encode Qwen3 inputs with last-token pooling and L2 normalization.

    Inputs use left padding. The encoder returns float16 embeddings.
    """

    def __init__(self, model_path: Path, device: int) -> None:
        """Load local Qwen weights and tokenizer on the selected CUDA device."""
        from transformers import AutoModel, AutoTokenizer

        tokenizers: _TokenizerFactory = AutoTokenizer
        models: _ModelFactory = AutoModel
        self._device = torch.device(f"cuda:{device}")
        self._tokenizer = tokenizers.from_pretrained(str(model_path), local_files_only=True, padding_side="left")
        self._model = models.from_pretrained(
            str(model_path),
            local_files_only=True,
            dtype=torch.bfloat16,
            attn_implementation="sdpa",
            device_map=str(self._device),
        )

    def tokenize(self, texts: Sequence[str]) -> DenseInputs:
        """Tokenize without truncation and pin input tensors in host memory."""
        encoded = self._tokenizer(list(texts), padding=True, truncation=False, return_tensors="pt")
        return DenseInputs(encoded["input_ids"].pin_memory(), encoded["attention_mask"].pin_memory())

    def forward(self, inputs: DenseInputs) -> NDArray[np.float16]:
        """Return float16 embeddings; raise BatchTooLargeError on GPU exhaustion."""
        values: _HalfTensor | None = None
        try:
            values = self._forward(inputs)
        except torch.OutOfMemoryError:
            pass
        if values is None:
            # Outside the except block the failed forward's frames and activations are unreachable,
            # so the cache release below can actually return their memory before the caller splits.
            torch.cuda.empty_cache()
            raise BatchTooLargeError
        return values.numpy()

    def _forward(self, inputs: DenseInputs) -> _HalfTensor:
        """Normalize final-token states and copy finite float16 vectors to CPU."""
        with torch.inference_mode():
            input_ids = inputs.input_ids.to(self._device, non_blocking=True)
            attention_mask = inputs.attention_mask.to(self._device, non_blocking=True)
            hidden = self._model(input_ids=input_ids, attention_mask=attention_mask).last_hidden_state
            pooled = torch.nn.functional.normalize(hidden[:, -1].float(), p=2.0, dim=1)
            if not bool(torch.isfinite(pooled).all()):
                raise ValueError("Dense model produced a nonfinite vector")
            values: _HalfTensor = pooled.to(torch.float16).cpu()
        return values


class _SparseOutput(TypedDict):
    lexical_weights: list[dict[str, float]]


class _SparseModel(Protocol):
    def encode(
        self,
        sentences: list[str],
        *,
        batch_size: int,
        max_length: int,
        return_dense: bool,
        return_sparse: bool,
        return_colbert_vecs: bool,
    ) -> _SparseOutput:
        """Encode texts with the requested dense, sparse, and ColBERT outputs."""
        ...


class BgeSparseEncoder:
    def __init__(self, model_path: Path, device: int, max_tokens: int, batch_size: int) -> None:
        """Load local BGE weights for sparse encoding on one CUDA device."""
        from FlagEmbedding import BGEM3FlagModel

        self._max_tokens, self._batch_size = max_tokens, batch_size
        self._model: _SparseModel = BGEM3FlagModel(
            str(model_path),
            devices=[f"cuda:{device}"],
            use_fp16=False,
            use_bf16=True,
            return_dense=False,
            return_sparse=True,
            return_colbert_vecs=False,
            passage_max_length=max_tokens,
        )

    def encode(self, texts: Sequence[str]) -> tuple[SparseWeights, ...]:
        """Return sparse weights sorted by integer token ID for each text."""
        weights = self._model.encode(
            list(texts),
            batch_size=self._batch_size,
            max_length=self._max_tokens,
            return_dense=False,
            return_sparse=True,
            return_colbert_vecs=False,
        )["lexical_weights"]
        rows: list[SparseWeights] = []
        for mapping in weights:
            ordered = sorted((int(key), float(value)) for key, value in mapping.items())
            rows.append(SparseWeights(tuple(key for key, _ in ordered), tuple(value for _, value in ordered)))
        return tuple(rows)


def run_gpu_worker(device: int, run_root: str, models: ModelPaths, settings: EmbedSettings, follow: bool) -> None:
    """Load both models on one GPU and embed shards until work is exhausted."""
    configure_embedding_output()
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    layout = RunLayout(Path(run_root))
    parent_alive = parent_process_alive()

    def body() -> None:
        """Load both GPU models, record load time, and process claimed shards."""
        torch.cuda.set_device(device)
        started = time.perf_counter()
        dense = QwenDenseEncoder(models.dense, device)
        sparse = BgeSparseEncoder(models.sparse, device, settings.max_tokens, settings.sparse_batch_size)
        atomic_write_json(layout.worker_ready(f"gpu{device}"), {"load_seconds": time.perf_counter() - started})
        batching = DenseBatching(settings.dense_batch_tokens, settings.dense_batch_rows, settings.dense_dimension)
        gpu_worker_loop(
            layout,
            lambda shard: embed_dense_shard(layout, shard, dense, batching, device),
            lambda shard: embed_sparse_shard(layout, shard, sparse, device),
            follow=follow,
            parent_alive=parent_alive,
        )

    guarded_worker(layout, f"gpu{device}", body)


def run_embed(
    layout: RunLayout, models: ModelPaths, settings: EmbedSettings, *, follow: bool, stop: Event | None = None
) -> None:
    """Run both models per GPU worker; stop all workers if one fails."""
    visible = torch.cuda.device_count()
    if max(settings.gpus) >= visible:
        raise RuntimeError(f"A configured GPU ordinal is not visible; {visible} CUDA devices are available")
    context = mp.get_context("spawn")
    workers: list[tuple[str, BaseProcess]] = [
        (
            f"gpu{device}",
            context.Process(
                target=run_gpu_worker,
                args=(device, str(layout.root), models, settings, follow),
                name=f"gpu{device}",
            ),
        )
        for device in settings.gpus
    ]
    supervise_processes(layout, workers, stop=stop)
