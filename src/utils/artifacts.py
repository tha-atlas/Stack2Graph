"""Publish binary files durably and clean up their private temporary files."""

import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import BinaryIO

from utils.types import CanonicalValue


def fsync_directory(path: Path) -> None:
    """Flush directory metadata to disk."""
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class AtomicBinaryWriter:
    def __init__(self, path: Path) -> None:
        """Open a temporary binary file beside a target without symlinks."""
        if path.is_symlink() or path.parent.resolve() != path.parent.absolute():
            raise ValueError(f"Refusing symlink artifact path: {path}")
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(prefix=f".{path.name}-", suffix=".tmp", dir=path.parent)
        self.temporary = Path(name)
        self.stream: BinaryIO = os.fdopen(descriptor, "wb")

    def publish(self) -> None:
        """Flush and close the stream, then atomically replace the target."""
        self.stream.flush()
        os.fsync(self.stream.fileno())
        self.stream.close()
        if self.path.is_symlink():
            raise ValueError(f"Refusing symlink artifact path: {self.path}")
        os.replace(self.temporary, self.path)
        fsync_directory(self.path.parent)

    def discard(self) -> None:
        """Close the stream and remove any remaining temporary file."""
        self.stream.close()
        self.temporary.unlink(missing_ok=True)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write bytes atomically and remove the temporary file on exit."""
    writer = AtomicBinaryWriter(path)
    try:
        writer.stream.write(data)
        writer.publish()
    finally:
        writer.discard()


def atomic_write_json(path: Path, value: Mapping[str, CanonicalValue], *, newline: bool = False) -> None:
    """Atomically write sorted JSON with an optional trailing newline."""
    separators = None if newline else (",", ":")
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=not newline, separators=separators)
    atomic_write_bytes(path, (encoded + ("\n" if newline else "")).encode("utf-8"))
