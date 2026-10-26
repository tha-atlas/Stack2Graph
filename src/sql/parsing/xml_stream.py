"""Stream validated XML without filtering, sanitizing, or ID assumptions."""

import re
from collections import deque
from collections.abc import Iterable, Iterator
from xml.parsers import expat

from sql.domain.models import Row, RowValue, TableBatch, XMLInput
from sql.domain.schema import TABLES, timestamp_ns
from sql.parsing.envelope import EnvelopeRules
from utils.resources import BatchLimits

INTEGER_PATTERN = re.compile(r"-?\d+", flags=re.ASCII)
KNOWN_ATTRIBUTES = {
    table: frozenset(column.name for column in spec.columns if column.name != "ExtraAttributes")
    for table, spec in TABLES.items()
}


def decode_row(table: str, attributes: dict[str, str], position: int) -> Row:
    """Validate XML attributes against the schema and retain unknown fields."""
    spec = TABLES[table]
    known = KNOWN_ATTRIBUTES[table]
    row: Row = {}
    for column in spec.columns:
        if column.name == "ExtraAttributes":
            continue
        raw = attributes.get(column.name)
        value: RowValue = raw
        try:
            if raw is None:
                if not column.nullable:
                    raise ValueError("Required attribute is absent")
            elif column.kind == "BIGINT":
                if not INTEGER_PATTERN.fullmatch(raw):
                    raise ValueError("Expected an integer")
                value = int(raw)
                if not -(1 << 63) <= value < 1 << 63:
                    raise ValueError("Integer outside signed 64-bit range")
            elif column.kind == "TIMESTAMP_TEXT":
                timestamp_ns(raw)
            elif column.kind == "BOOLEAN":
                if raw not in ("true", "false", "True", "False", "1", "0"):
                    raise ValueError("Expected a boolean")
                value = raw in ("true", "True", "1")
        except ValueError as error:
            raise ValueError(f"{table} position {position} field {column.name}: {error}") from error
        row[column.name] = value
    extra = {key: value for key, value in attributes.items() if key not in known}
    row["ExtraAttributes"] = extra or None
    return row


def payload_bytes(attributes: dict[str, str]) -> int:
    # Includes object/column overhead; COPY conversion has a separate RAM reserve.
    """Estimate decoded attribute bytes including per-column overhead."""
    return 128 + sum(64 + len(key.encode("utf-8")) + len(value.encode("utf-8")) for key, value in attributes.items())


class XMLTableStream:
    def __init__(self, source: XMLInput, limits: BatchLimits, skip_rows: int = 0) -> None:
        """Set the XML source, batching limits, and validated resume position."""
        if source.table not in TABLES or skip_rows < 0:
            raise ValueError("Unsupported table or negative resume position")
        self.source, self.limits, self.skip_rows = source, limits, skip_rows
        self.xml_rows: int | None = None

    def __iter__(self) -> Iterator[TableBatch]:
        """Yield bounded batches from the XML input file."""
        with self.source.path.open("rb") as stream:
            yield from self.parse_chunks(iter(lambda: stream.read(min(64 * 1024, self.limits.max_record_bytes)), b""))

    def parse_chunks(self, chunks: Iterable[bytes], initial_position: int = 0) -> Iterator[TableBatch]:
        """Parse a complete document, optionally offsetting fragment row positions."""
        parser = expat.ParserCreate("UTF-8")
        parser.buffer_text = False
        rules = EnvelopeRules(self.source.table, TABLES[self.source.table].xml_root, self.limits.max_record_bytes)
        queue: deque[tuple[int, Row, int]] = deque()
        depth, position, root_seen, root_closed = 0, initial_position, False, False
        last_event = 0

        def start(name: str, attributes: dict[str, str]) -> None:
            """Validate the XML envelope and queue decoded rows after the resume point."""
            nonlocal depth, position, root_seen, last_event
            last_event = parser.CurrentByteIndex
            if depth == 0:
                rules.check_root(root_seen, name, attributes)
                root_seen = True
            elif depth == 1 and name == "row":
                position += 1
                size = payload_bytes(attributes)
                if size > self.limits.max_record_bytes:
                    raise ValueError(f"{self.source.table} position {position}: exceeds record budget ({size} bytes)")
                # Validate skipped rows too; changed or damaged XML cannot masquerade as a resume.
                row = decode_row(self.source.table, attributes, position)
                if position > self.skip_rows:
                    queue.append((position, row, size))
            else:
                rules.nested(position, name)
            depth += 1

        def end(name: str) -> None:
            """Track element depth and whether the root has closed."""
            nonlocal depth, root_closed, last_event
            depth -= 1
            last_event = parser.CurrentByteIndex
            if depth == 0:
                root_closed = True

        def text(value: str) -> None:
            """Reject non-whitespace text and update the parser byte offset."""
            nonlocal last_event
            last_event = parser.CurrentByteIndex
            rules.text(position, value)

        parser.StartElementHandler, parser.EndElementHandler, parser.CharacterDataHandler = start, end, text
        rules.guard(parser)
        rows: list[Row] = []
        size, first = 0, 0
        consumed = 0
        try:

            def terminated_chunks() -> Iterator[bytes]:
                """Yield input chunks followed by an empty chunk to finalize parsing."""
                yield from chunks
                yield b""

            for chunk in terminated_chunks():
                consumed += len(chunk)
                parser.Parse(chunk, not chunk)
                rules.check_token_budget(consumed, last_event, depth, position, root_closed)
                while queue:
                    row_position, row, row_size = queue.popleft()
                    if rows and (len(rows) >= self.limits.max_rows or size + row_size > self.limits.max_bytes):
                        yield TableBatch(self.source.table, first, tuple(rows))
                        rows, size = [], 0
                    if not rows:
                        first = row_position
                    rows.append(row)
                    size += row_size
            if not root_seen or not root_closed or depth or position < self.skip_rows:
                raise ValueError(f"{self.source.table}: incomplete XML or resume exceeds XML row count")
            if rows:
                yield TableBatch(self.source.table, first, tuple(rows))
            self.xml_rows = position
        except expat.ExpatError as error:
            raise rules.malformed(position, error) from error


def iter_batches(source: XMLInput, limits: BatchLimits, skip_rows: int = 0) -> Iterator[TableBatch]:
    """Return a bounded XML table iterator at the requested resume position."""
    return iter(XMLTableStream(source, limits, skip_rows))
