"""Split normalized text without loss and check both model token budgets."""

import re
from dataclasses import dataclass
from html.parser import HTMLParser

from vector.domain.config import ChunkConfig
from vector.domain.models import ChildChunk, PreparedChunk, SelectedQuestion
from vector.interfaces.token_budget import TokenBudget

BLOCK_TAGS = frozenset(("p", "div", "pre", "li", "h1", "h2", "h3", "h4", "blockquote", "table", "tr"))


class ChunkingError(ValueError):
    """Report that question text cannot fit the configured chunk budgets."""


@dataclass(frozen=True)
class NormalizedBody:
    text: str
    code_ranges: tuple[tuple[int, int], ...]


class _BodyParser(HTMLParser):
    def __init__(self) -> None:
        """Initialize text offsets, code ranges, and hidden-element tracking."""
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.size = 0
        self.code_depth = 0
        self.hidden_depth = 0
        self.code_start = 0
        self.code_ranges: list[tuple[int, int]] = []

    def _append(self, text: str) -> None:
        """Append text and advance the normalized character count."""
        self.parts.append(text)
        self.size += len(text)

    def _break(self) -> None:
        """Append a newline unless the text is empty or already ends with one."""
        if self.parts and not self.parts[-1].endswith("\n"):
            self._append("\n")

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Track hidden and code elements and add block separators."""
        del attrs
        if tag in ("script", "style"):
            self.hidden_depth += 1
        if self.hidden_depth:
            return
        if tag in BLOCK_TAGS or tag == "br":
            self._break()
        if tag in ("pre", "code"):
            if not self.code_depth:
                self.code_start = self.size
            self.code_depth += 1

    def handle_endtag(self, tag: str) -> None:
        """Close hidden or code ranges and add block separators."""
        if tag in ("script", "style") and self.hidden_depth:
            self.hidden_depth -= 1
            return
        if self.hidden_depth:
            return
        if tag in ("pre", "code") and self.code_depth:
            self.code_depth -= 1
            if not self.code_depth:
                self.code_ranges.append((self.code_start, self.size))
        if tag in BLOCK_TAGS:
            self._break()

    def handle_data(self, data: str) -> None:
        """Keep visible text, preserving code whitespace and collapsing prose."""
        if not self.hidden_depth:
            self._append(data if self.code_depth else re.sub(r"\s+", " ", data))


def normalize_body(html: str) -> NormalizedBody:
    """Extract visible HTML text and the character ranges of code blocks."""
    parser = _BodyParser()
    parser.feed(html)
    parser.close()
    if parser.code_depth:
        parser.code_ranges.append((parser.code_start, parser.size))
    return NormalizedBody("".join(parser.parts), tuple(parser.code_ranges))


def _counts(text: str, budgets: tuple[TokenBudget, TokenBudget]) -> tuple[int, int]:
    """Return input-token counts for both model budgets."""
    return budgets[0].count_input_tokens(text), budgets[1].count_input_tokens(text)


def _fits(text: str, budgets: tuple[TokenBudget, TokenBudget], limit: int) -> bool:
    """Return whether text fits both model token budgets."""
    return all(budget.count_input_tokens(text) <= limit for budget in budgets)


def _end(text: str, start: int, prefix: str, budgets: tuple[TokenBudget, TokenBudget], limit: int) -> int:
    """Find a text end offset that fits both budgets with the prefix."""
    low, high = start, min(len(text), start + limit * 4)
    while high < len(text) and _fits(prefix + text[start:high], budgets, limit):
        low, high = high, min(len(text), start + (high - start) * 2)
    if _fits(prefix + text[start:high], budgets, limit):
        return high
    while low + 1 < high:
        middle = (low + high) // 2
        if _fits(prefix + text[start:middle], budgets, limit):
            low = middle
        else:
            high = middle
    return low


def _overlap_start(body: NormalizedBody, end: int, budgets: tuple[TokenBudget, TokenBudget], limit: int) -> int:
    """Find a token-bounded overlap start that avoids splitting code ranges."""
    if not limit:
        return end
    low, high = max(0, end - limit * 4), end
    while low < high:
        middle = (low + high) // 2
        if _fits(body.text[middle:end], budgets, limit):
            high = middle
        else:
            low = middle + 1
    for code_start, code_end in body.code_ranges:
        if code_start < end and code_end > low:
            low = max(low, min(code_end, end))
    return low


def prepare_question(
    selected: SelectedQuestion,
    config: ChunkConfig,
    token_budgets: tuple[TokenBudget, TokenBudget],
    *,
    max_prepared_bytes: int | None = None,
) -> tuple[PreparedChunk, ...]:
    """
    Build lossless children and retain both final validated input lengths.

    Args:
        selected: Question and its selected language memberships.
        config: Maximum input tokens and overlap allowance.
        token_budgets: Dense and sparse model input counters, in that order.
        max_prepared_bytes: Optional total byte cap for the prepared children.

    Returns:
        Lossless child chunks with both validated model input lengths.

    Raises:
        ChunkingError: Content cannot fit the token or prepared-byte limits.

    """
    prefix = f"Title: {selected.question.title}\n\n"
    prefix_counts = _counts(prefix, token_budgets)
    if max(prefix_counts) > config.max_tokens:
        raise ChunkingError(f"Question {selected.question.question_id}: title context exceeds input budget")
    body = normalize_body(selected.question.body_html)
    if not body.text:
        if not selected.question.title:
            raise ChunkingError(f"Question {selected.question.question_id}: no visible title or body")
        return (PreparedChunk(ChildChunk(selected, 0, 1, prefix, "", 0, 0, 0), *prefix_counts),)
    spans: list[tuple[int, int, int, int, int]] = []
    prepared_bytes = len(body.text) * 4
    prefix_bytes = len(prefix) * 4
    consumed, start = 0, 0
    while consumed < len(body.text):
        end = _end(body.text, start, prefix, token_budgets, config.max_tokens)
        if end <= consumed and start < consumed:
            start = consumed
            end = _end(body.text, start, prefix, token_budgets, config.max_tokens)
        if end <= consumed:
            raise ChunkingError(f"Question {selected.question.question_id}: body cannot fit after title context")
        if end < len(body.text):
            newline = body.text.rfind("\n", consumed, end)
            if newline >= consumed:
                end = newline + 1
        # Retest structural cuts: tokenizer counts need not be monotonic in prefix length.
        final_counts = _counts(prefix + body.text[start:end], token_budgets)
        while end > consumed and max(final_counts) > config.max_tokens:
            end -= 1
            if end > consumed:
                final_counts = _counts(prefix + body.text[start:end], token_budgets)
        if end <= consumed:
            raise ChunkingError(f"Question {selected.question.question_id}: no valid lossless split")
        prepared_bytes += 1024 + prefix_bytes + (end - start) * 8
        if max_prepared_bytes is not None and prepared_bytes > max_prepared_bytes:
            raise ChunkingError(f"Question {selected.question.question_id}: prepared byte budget exceeded")
        spans.append((start, end, consumed - start, *final_counts))
        consumed = end
        if consumed < len(body.text):
            start = _overlap_start(body, end, token_budgets, config.overlap)
    return tuple(
        PreparedChunk(
            ChildChunk(
                selected, index, len(spans), prefix + body.text[start:end], body.text[start:end], start, end, overlap
            ),
            dense_tokens,
            sparse_tokens,
        )
        for index, (start, end, overlap, dense_tokens, sparse_tokens) in enumerate(spans)
    )


def chunk_question(
    selected: SelectedQuestion,
    config: ChunkConfig,
    token_budgets: tuple[TokenBudget, TokenBudget],
    *,
    max_prepared_bytes: int | None = None,
) -> tuple[ChildChunk, ...]:
    """
    Return the public notebook child records without preparation metadata.

    Args:
        selected: Question and its selected language memberships.
        config: Maximum input tokens and overlap allowance.
        token_budgets: Dense and sparse model input counters, in that order.
        max_prepared_bytes: Optional total byte cap for prepared children.

    Returns:
        Child records without internal token-count preparation metadata.

    """
    return tuple(
        prepared.chunk
        for prepared in prepare_question(selected, config, token_budgets, max_prepared_bytes=max_prepared_bytes)
    )
