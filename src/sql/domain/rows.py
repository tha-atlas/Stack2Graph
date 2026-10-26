"""Validate dump-row values independently of the database driver."""

from sql.domain.models import Row


def integer(row: Row | None, key: str) -> int:
    """Read an integer column, rejecting nulls, booleans, and other types."""
    value = row.get(key) if row is not None else None
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"Expected PostgreSQL integer column: {key}")
    return value


def text(row: Row | None, key: str) -> str:
    """Read a required text column or raise ValueError."""
    value = row.get(key) if row is not None else None
    if not isinstance(value, str):
        raise ValueError(f"Expected PostgreSQL text column: {key}")
    return value


def optional_integer(row: Row, key: str) -> int | None:
    """Read an optional integer column, preserving missing or null values."""
    return None if row.get(key) is None else integer(row, key)


def optional_text(row: Row, key: str) -> str | None:
    """Read an optional text column, preserving missing or null values."""
    return None if row.get(key) is None else text(row, key)
