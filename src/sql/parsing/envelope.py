"""Enforce the same XML document rules in both table parsers."""

from typing import NoReturn
from xml.parsers import expat


class EnvelopeRules:
    """
    Enforce shared XML document rules for streaming and block parsers.

    Attribute an unfinished oversized token according to parser state. Inside a
    row whose start Expat has reported (depth >= 2), use that row. After the
    root closes, use the last row. Before the root, between rows, or inside an
    unreported start tag, use the next row (position + 1). Adding one
    unconditionally would assign errors inside an open row to the following
    row.
    """

    def __init__(self, table: str, root: str, max_record_bytes: int) -> None:
        """Set the expected XML root and the maximum token size."""
        self.table = table
        self.root = root
        self.token_limit = max_record_bytes + 64 * 1024

    def guard(self, parser: expat.XMLParserType) -> None:
        """Install handlers that reject DTD declarations and entities."""
        parser.StartDoctypeDeclHandler = self.reject
        parser.EntityDeclHandler = self.reject
        parser.ExternalEntityRefHandler = self.reject

    def reject(self, *_arguments: str | int | None) -> NoReturn:
        """Raise ValueError for a DTD or entity declaration."""
        raise ValueError(f"{self.table}: DTDs and entities are forbidden")

    def check_root(self, root_seen: bool, name: str, attributes: dict[str, str]) -> None:
        """Require a single root with the expected name and no attributes."""
        if root_seen or name != self.root or attributes:
            raise ValueError(f"{self.table}: unexpected XML root or attributes")

    def nested(self, position: int, name: str) -> NoReturn:
        """Raise a position-specific error for an unexpected nested element."""
        raise ValueError(f"{self.table} position {position}: unexpected nested XML element {name}")

    def text(self, position: int, value: str) -> None:
        """Reject non-whitespace text between XML rows."""
        if value.strip():
            raise ValueError(f"{self.table} position {position}: unexpected XML text")

    def check_token_budget(self, consumed: int, last_event: int, depth: int, position: int, root_closed: bool) -> None:
        """Reject an XML token whose bytes exceed the configured allowance."""
        if consumed - last_event > self.token_limit:
            row = position if depth >= 2 or root_closed else position + 1
            raise ValueError(f"{self.table} position {row}: XML token exceeds record budget")

    def malformed(self, position: int, error: expat.ExpatError) -> ValueError:
        """Build a table- and position-specific error for malformed XML."""
        return ValueError(f"{self.table} position {position}: malformed XML: {error}")
