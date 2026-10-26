from typing import Protocol, TypedDict

from torch.nn import Module

class Tokenizer(Protocol):
    def encode(self, text: str, *, add_special_tokens: bool, truncation: bool) -> list[int]: ...

class SparseOutput(TypedDict):
    lexical_weights: list[dict[str, float]]

class BGEM3FlagModel:
    tokenizer: Tokenizer
    model: Module
    def __init__(
        self,
        model_name_or_path: str,
        *,
        devices: list[str],
        use_fp16: bool,
        use_bf16: bool,
        return_dense: bool,
        return_sparse: bool,
        return_colbert_vecs: bool,
        passage_max_length: int,
    ) -> None: ...
    def encode(
        self,
        sentences: list[str],
        *,
        batch_size: int,
        max_length: int,
        return_dense: bool,
        return_sparse: bool,
        return_colbert_vecs: bool,
    ) -> SparseOutput: ...
    def stop_self_pool(self) -> None: ...
