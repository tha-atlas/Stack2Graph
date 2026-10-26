"""Resolve the language registry used by Vector, Graph, and source audits."""

from collections.abc import Mapping
from pathlib import Path


def registry_path(values: Mapping[str, str]) -> Path:
    """Resolve the selection registry path from the environment."""
    return Path(
        values.get("STACK2GRAPH_SELECTION_REGISTRY_PATH", str(Path(__file__).with_name("registry.json")))
    ).resolve()
