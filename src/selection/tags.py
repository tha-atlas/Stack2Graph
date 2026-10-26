"""Parse source tag tokens and reject incomplete or malformed input."""

import re

# Single owner of the tag token grammar; SQL adapters interpolate this exact text
# into regexp_matches expressions (SQL extracts matches, Python validates the whole).
TAG_TOKEN_PATTERN = r"<([^<>]+)>"


def parse_tags(raw: str | None) -> tuple[str, ...]:
    """Parse complete angle-bracket tag tokens; reject malformed input."""
    if raw is None:
        return ()
    tags: tuple[str, ...] = tuple(match.group(1) for match in re.finditer(TAG_TOKEN_PATTERN, raw))
    if "".join(f"<{tag}>" for tag in tags) != raw or any(
        any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in tag) for tag in tags
    ):
        raise ValueError("Question tags must consist of complete tag tokens")
    return tags
