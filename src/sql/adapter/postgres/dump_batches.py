"""Encode bounded COPY text batches without connections or staging files."""

import json

from sql.domain.models import EncodedTableBatch, RowValue, TableBatch
from sql.domain.schema import TABLES, Column, timestamp_ns
from utils.resources import BatchLimits


def copy_value(value: RowValue, column: Column) -> str:
    """Validate a column value and escape it for PostgreSQL text COPY."""
    if value is None:
        if not column.nullable:
            raise ValueError(f"Required column is NULL: {column.name}")
        return r"\N"
    if column.kind == "BIGINT":
        if not isinstance(value, int) or isinstance(value, bool) or not -(1 << 63) <= value < 1 << 63:
            raise ValueError(f"Expected signed BIGINT: {column.name}")
        return str(value)
    if column.kind == "BOOLEAN":
        if not isinstance(value, bool):
            raise ValueError(f"Expected boolean: {column.name}")
        return "t" if value else "f"
    if column.kind == "JSON":
        if not isinstance(value, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in value.items()):
            raise ValueError("ExtraAttributes must contain original string attributes")
        raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    else:
        if not isinstance(value, str) or "\x00" in value:
            raise ValueError(f"Expected PostgreSQL text without NUL: {column.name}")
        if column.kind == "TIMESTAMP_TEXT":
            timestamp_ns(value)
        raw = value
    return raw.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def encode_batch(batch: TableBatch, limits: BatchLimits) -> EncodedTableBatch:
    """Validate row and byte limits and encode a complete COPY batch."""
    if batch.table not in TABLES or not batch.rows or len(batch.rows) > limits.max_rows:
        raise ValueError("Unsupported table, empty batch or row budget exceeded")
    spec = TABLES[batch.table]
    names = {column.name for column in spec.columns}
    chunks: list[bytes] = []
    size = 0
    for row in batch.rows:
        if set(row) != names:
            raise ValueError("Batch columns do not match complete logical schema")
        chunk = ("\t".join(copy_value(row[column.name], column) for column in spec.columns) + "\n").encode("utf-8")
        if len(chunk) > limits.max_record_bytes * 2:
            raise ValueError("Escaped COPY record exceeds byte budget")
        size += len(chunk)
        if size > limits.max_bytes * 2:
            raise ValueError("Escaped COPY batch exceeds byte budget")
        chunks.append(chunk)
    return EncodedTableBatch(batch.table, batch.first_position, len(batch.rows), b"".join(chunks))
