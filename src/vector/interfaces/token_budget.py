"""Define model input-token counts independently of SDK types."""

from typing import Protocol


class TokenBudget(Protocol):
    def count_input_tokens(self, text: str) -> int:
        """Count model input tokens, including required special tokens."""
        ...
