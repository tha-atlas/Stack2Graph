"""Measure both models' input lengths using their fixed local tokenizers."""

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from vector.interfaces.token_budget import TokenBudget


class _Tokenizer(Protocol):
    def encode(self, text: str, *, add_special_tokens: bool, truncation: bool) -> list[int]:
        """Encode text with explicit special-token and truncation policies."""
        ...


class _TokenizerFactory(Protocol):
    def from_pretrained(self, pretrained_model_name_or_path: str, *, local_files_only: bool) -> _Tokenizer:
        """Load a tokenizer from the requested model path."""
        ...


@dataclass(frozen=True)
class ModelTokenBudget:
    tokenizer: _Tokenizer

    def count_input_tokens(self, text: str) -> int:
        """Count tokens including special tokens without truncating text."""
        return len(self.tokenizer.encode(text, add_special_tokens=True, truncation=False))


def load_token_budgets(dense_path: Path, sparse_path: Path) -> tuple[TokenBudget, TokenBudget]:
    """Load local dense and sparse tokenizers for input-length checks."""
    from transformers import AutoTokenizer

    factory: _TokenizerFactory = AutoTokenizer
    return (
        ModelTokenBudget(factory.from_pretrained(str(dense_path), local_files_only=True)),
        ModelTokenBudget(factory.from_pretrained(str(sparse_path), local_files_only=True)),
    )
