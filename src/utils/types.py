"""Define shared JSON values and canonical serialization types."""

from collections.abc import Mapping, Sequence
from typing import Protocol, TypeAlias, cast

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
JSONObject: TypeAlias = dict[str, JSONValue]
CanonicalValue: TypeAlias = JSONScalar | Sequence["CanonicalValue"] | Mapping[str, "CanonicalValue"]


class _JSONDecoder(Protocol):
    def decode(self, text: str, /) -> JSONValue:
        """Decode a JSON document into typed JSON values."""
        ...


def parse_json(text: str) -> JSONValue:
    """Decode JSON into the project's JSON value types."""
    import json

    decoder: _JSONDecoder = json.JSONDecoder()
    return decoder.decode(text)


def json_object(value: JSONValue) -> JSONObject:
    """Require a JSON object and return its typed field mapping."""
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object")
    return value


def json_string(value: JSONValue) -> str:
    """Return a JSON string or raise ValueError for another value type."""
    if not isinstance(value, str):
        raise ValueError("Expected a JSON string")
    return value


def json_integer(value: JSONValue) -> int:
    """Return a JSON integer, rejecting booleans and other value types."""
    if type(value) is not int:
        raise ValueError("Expected a JSON integer")
    return value


def json_array(value: JSONValue) -> list[JSONValue]:
    """Return a JSON array or raise ValueError for another value type."""
    if not isinstance(value, list):
        raise ValueError("Expected a JSON array")
    return value


def decoded_json(value: CanonicalValue) -> JSONValue:
    """Normalize JSON-compatible tuples/mappings through the standard encoder."""
    import json

    return cast(JSONValue, json.loads(json.dumps(value)))


def canonical_hash(value: CanonicalValue, *, ensure_ascii: bool = True, allow_nan: bool = True) -> str:
    """Hash sorted compact JSON, preserving each persisted format's encoding policy."""
    import json
    from hashlib import sha256

    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=ensure_ascii, allow_nan=allow_nan)
    return sha256(encoded.encode("utf-8")).hexdigest()
