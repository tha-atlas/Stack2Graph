"""Sort inputs by length and limit padded tokens and rows per batch."""

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Batch:
    rows: tuple[int, ...]
    max_tokens: int


def plan_batches(token_counts: Sequence[int], max_batch_tokens: int, max_batch_rows: int) -> tuple[Batch, ...]:
    """Sort rows by token count (ties by index) and cut batches at either budget."""
    if min(max_batch_tokens, max_batch_rows) <= 0:
        raise ValueError("Batch token and row budgets must be positive")
    for index, tokens in enumerate(token_counts):
        if not 0 < tokens <= max_batch_tokens:
            raise ValueError(f"Row {index} has {tokens} tokens; the batch budget is {max_batch_tokens}")

    def order_key(index: int) -> tuple[int, int]:
        """Sort by token count, using the original index to break ties."""
        return token_counts[index], index

    order = sorted(range(len(token_counts)), key=order_key)
    batches: list[Batch] = []
    current: list[int] = []
    for index in order:
        tokens = token_counts[index]
        if current and (len(current) + 1 > max_batch_rows or (len(current) + 1) * tokens > max_batch_tokens):
            batches.append(Batch(tuple(current), token_counts[current[-1]]))
            current = []
        current.append(index)
    if current:
        batches.append(Batch(tuple(current), token_counts[current[-1]]))
    return tuple(batches)
