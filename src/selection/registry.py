"""Load whole-tag matching rules and retain their source text and digest."""

import re
from hashlib import sha256
from pathlib import Path

from selection.models import SelectionRegistry
from utils.types import json_array, json_object, json_string, parse_json


def load_selection_registry(path: Path) -> SelectionRegistry:
    """
    Load a version 1 registry and reject unsafe or inconsistent rules.

    Args:
        path: UTF-8 JSON registry containing base tags, versions, and synonyms.

    Returns:
        Validated rules with the original text and its SHA256 digest.

    Raises:
        ValueError: The version, categories, patterns, or aliases are invalid.

    """
    raw = path.read_bytes()
    content = raw.decode("utf-8")
    document = json_object(parse_json(content))
    if json_string(document["version"]) != "1":
        raise ValueError("Unsupported selection registry version")
    base = tuple(json_string(value) for value in json_array(document["base_tags"]))
    if not base or len(set(base)) != len(base):
        raise ValueError("Registry categories must be nonempty and unique")
    for category in base:
        if (
            category in ("schema", "extra-languages", ".", "..")
            or not category
            or any(character in category for character in "/\\")
            or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in category)
        ):
            raise ValueError(f"Unsafe or reserved registry category: {category!r}")
    versions: list[tuple[str, re.Pattern[str]]] = []
    synonyms: list[tuple[str, str]] = []
    for item in json_array(document["version_rules"]):
        rule = json_object(item)
        language, pattern = json_string(rule["language"]), json_string(rule["pattern"])
        if language not in base or not pattern.startswith("^") or not pattern.endswith("$"):
            raise ValueError("Language version rules must match complete tags")
        try:
            compiled = re.compile(pattern)
        except re.error as error:
            raise ValueError(f"Invalid language version pattern: {pattern}") from error
        if (language, compiled) in versions:
            raise ValueError("Duplicate version rule")
        versions.append((language, compiled))
    for item in json_array(document["synonyms"]):
        rule = json_object(item)
        alias, language = json_string(rule["alias"]), json_string(rule["language"])
        evidence, date = json_string(rule["approval_evidence"]), json_string(rule["source_date"])
        if language not in base or not alias or not evidence or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
            raise ValueError("Synonyms require an allowed language and dated approval evidence")
        synonyms.append((alias, language))
    if len({alias for alias, _ in synonyms}) != len(synonyms):
        raise ValueError("Duplicate synonym mapping")
    return SelectionRegistry(base, tuple(versions), tuple(synonyms), sha256(raw).hexdigest(), content)
