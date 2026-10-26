"""Read declared shard compression and hash uncompressed content separately."""

import gzip
from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO, Literal


@contextmanager
def open_shard(path: Path, compression: Literal["none", "gzip"]) -> Iterator[BinaryIO | gzip.GzipFile]:
    """Provide a binary shard stream with optional gzip decompression."""
    if compression == "gzip":
        with gzip.open(path, "rb") as stream:
            yield stream
    else:
        with path.open("rb") as stream:
            yield stream


def content_digest(path: Path, compression: Literal["none", "gzip"]) -> str:
    """Hash a shard's uncompressed content with SHA256."""
    digest = sha256()
    with open_shard(path, compression) as stream:
        while block := stream.read(1024**2):
            digest.update(block)
    return digest.hexdigest()
