"""Stream cataloged artifacts using Python's standard library in QLever."""

import gzip
from hashlib import sha256
from pathlib import Path
from typing import Protocol

from utils.types import json_integer, json_object, json_string, parse_json


class ByteSink(Protocol):
    def write(self, data: bytes, /) -> int:
        """Write bytes to the output sink and return the written count."""
        ...


def stream_catalog(catalog: Path, output: ByteSink) -> None:
    """Verify catalog files and stream their uncompressed bytes to output."""
    with catalog.open() as records:
        for line in records:
            value = json_object(parse_json(line))
            relative = Path(json_string(value["path"]))
            if ".." in relative.parts:
                raise ValueError("Producer path must not traverse directories")
            path = relative if relative.is_absolute() else catalog.parent / relative
            expected = json_string(value["sha256"])
            expected_size = json_integer(value["bytes"])
            if not path.is_absolute() or path.resolve() != path or not path.is_file():
                raise ValueError("Producer path must be an existing absolute file without symlinks")
            digest, size = sha256(), 0
            compression = json_string(value.get("compression", "none"))
            with path.open("rb") as stream:
                while block := stream.read(1024**2):
                    digest.update(block)
                    size += len(block)
                    if compression == "none":
                        output.write(block)
            if digest.hexdigest() != expected or size != expected_size:
                raise ValueError(f"Producer artifact changed: {path}")
            if compression == "gzip":
                with gzip.open(path, "rb") as stream:
                    while block := stream.read(1024**2):
                        output.write(block)
            elif compression != "none":
                raise ValueError("Unknown producer compression")
