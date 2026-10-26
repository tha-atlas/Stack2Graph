"""Persist JSON manifests and hold kernel-managed writer locks."""

from __future__ import annotations

import fcntl
import json
import os
from collections.abc import Mapping
from pathlib import Path
from types import TracebackType
from typing import Self, TextIO, cast

from utils.artifacts import atomic_write_json
from utils.types import CanonicalValue, JSONObject, JSONValue, json_object


class ManifestStore:
    def __init__(self, path: Path) -> None:
        """Store the manifest path without opening it."""
        self.path = path

    def read(self) -> JSONObject:
        """Read a bounded JSON manifest, returning an empty dict if absent."""
        if not self.path.exists():
            return {}
        if self.path.is_symlink():
            raise ValueError(f"Refusing symlink manifest: {self.path}")
        if self.path.stat().st_size > 16 * 1024 * 1024:
            raise ValueError("Manifest exceeds metadata budget")
        return json_object(cast(JSONValue, json.loads(self.path.read_text())))

    def write(self, state: Mapping[str, CanonicalValue]) -> None:
        """Atomically replace the manifest with sorted JSON and a newline."""
        atomic_write_json(self.path, state, newline=True)


class FileLock:
    """Hold a nonblocking process lock, reject symlinks, and release it on exit."""

    def __init__(self, path: Path, busy_message: str) -> None:
        """Set the lock path and the error message for a busy lock."""
        self.path = path
        self.busy_message = busy_message
        self.stream: TextIO | None = None

    def __enter__(self) -> Self:
        """Acquire a nonblocking writer lock and record this process's PID."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        self.stream = os.fdopen(descriptor, "r+")
        try:
            fcntl.flock(self.stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self.stream.close()
            self.stream = None
            if isinstance(error, BlockingIOError):
                raise RuntimeError(self.busy_message) from error
            raise
        try:
            self.stream.seek(0)
            self.stream.truncate()
            self.stream.write(str(os.getpid()))
            self.stream.flush()
        except BaseException:
            self.stream.close()
            self.stream = None
            raise
        return self

    def __exit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the lock stream to release the writer lock."""
        if self.stream:
            self.stream.close()
            self.stream = None


class SnapshotLock(FileLock):
    def __init__(self, snapshot_dir: Path) -> None:
        """Set up the writer lock for a snapshot directory."""
        super().__init__(snapshot_dir / "writer.lock", f"Snapshot already has an active writer: {snapshot_dir.name}")


def writer_active(snapshot_dir: Path) -> bool:
    """Return whether a snapshot's writer lock is currently held."""
    path = snapshot_dir / "writer.lock"
    if not path.exists():
        return False
    with path.open("r") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
    return False
