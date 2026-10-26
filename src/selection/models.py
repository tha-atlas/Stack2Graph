"""Represent reviewed language tags, version rules, and synonyms."""

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class SelectionRegistry:
    base_tags: tuple[str, ...]
    versions: tuple[tuple[str, re.Pattern[str]], ...]
    synonyms: tuple[tuple[str, str], ...]
    digest: str
    source_text: str
