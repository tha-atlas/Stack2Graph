"""Define the query transport used by graph consumers."""

from typing import Protocol

from graph.adapter.sparql import SelectResult


class SelectClient(Protocol):
    def select(self, query: str) -> SelectResult:
        """Execute a SELECT query and return typed variable bindings."""
        ...
