"""Check archive identity, HTTP ranges, and integrity during bounded transfers."""

import hashlib
import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from http.client import IncompleteRead
from pathlib import Path
from typing import Literal, Protocol
from urllib.error import HTTPError

from sql.archive.source import ArchiveSource, HTTPResponse, TransferClient
from sql.domain.models import ArchiveChecksum, SnapshotIdentity
from utils.artifacts import fsync_directory
from utils.backoff import archive_http_delay
from utils.config import SnapshotPaths
from utils.logging_utils import ProgressReporter, format_bytes, get_logger, stage_event
from utils.manifest import ManifestStore
from utils.resources import require_disk
from utils.types import json_string

LOGGER = get_logger(__name__)


def _valid_http_date(value: str | None) -> str | None:
    """Return a timezone-aware HTTP date unchanged, or None if invalid."""
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        return value if parsed.tzinfo is not None else None
    except ValueError, TypeError, OverflowError:
        return None


class Checksum(Protocol):
    def update(self, data: bytes, /) -> None:
        """Add bytes to the running checksum."""
        ...

    def hexdigest(self) -> str:
        """Return the current digest as hexadecimal text."""
        ...


def _hashes(identity: SnapshotIdentity) -> dict[str, Checksum]:
    """Create the published checksum algorithms plus a SHA256 hasher."""
    algorithms = {algorithm for algorithm, _ in identity.checksums} | {"sha256"}
    if not identity.checksums or not algorithms <= {"md5", "sha1", "sha256"}:
        raise ValueError("Download identity must contain supported checksums")
    hashes: dict[str, Checksum] = {}
    for algorithm in algorithms:
        hashes[algorithm] = hashlib.new(algorithm)
    return hashes


def _digest_file(path: Path, hashes: dict[str, Checksum], *, label: str, total: int, buffer_bytes: int) -> None:
    """Hash a whole file through every digest, reporting byte-wise progress."""
    with ProgressReporter(LOGGER, label, total=total).track() as progress:
        position = 0
        with path.open("rb") as stream:
            while chunk := stream.read(buffer_bytes):
                for digest in hashes.values():
                    digest.update(chunk)
                position += len(chunk)
                progress.update(position, total)
        progress.update(position, total, force=True)


def verify_archive(path: Path, identity: SnapshotIdentity, buffer_bytes: int = 1024**2) -> ArchiveChecksum:
    """Check file type, size, and published checksums; return its SHA256."""
    hashes = _hashes(identity)
    if path.is_symlink() or not path.is_file() or path.stat().st_size != identity.size:
        raise ValueError(f"Archive size/type mismatch: {path}")
    LOGGER.info("Verify archive checksums: %s", format_bytes(identity.size))
    _digest_file(path, hashes, label="Archive verification", total=identity.size, buffer_bytes=buffer_bytes)
    for algorithm, expected_digest in identity.checksums:
        if hashes[algorithm].hexdigest() != expected_digest:
            raise ValueError(f"Archive {algorithm} checksum mismatch: {path}")
    return ArchiveChecksum(identity.size, hashes["sha256"].hexdigest())


@dataclass
class ArchiveVerification:
    """Reuse a run's checksum only while file identity and metadata match."""

    path: Path
    identity: SnapshotIdentity
    checksum: ArchiveChecksum | None = None
    file_state: tuple[int, int, int, int, int] | None = None

    def _state(self) -> tuple[int, int, int, int, int]:
        """Read a regular archive's device, inode, size, and change timestamps."""
        if self.path.is_symlink() or not self.path.is_file():
            raise ValueError(f"Archive size/type mismatch: {self.path}")
        stat = self.path.stat()
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns

    def verify(self) -> ArchiveChecksum:
        """Reuse an unchanged file's checksum or verify it and cache its state."""
        state = self._state()
        if self.checksum is not None and state == self.file_state:
            LOGGER.debug("Reuse this run's verified archive checksum: %s", self.path)
            return self.checksum
        result = verify_archive(self.path, self.identity)
        if self._state() != state:
            raise ValueError(f"Archive changed during checksum verification: {self.path}")
        self.checksum, self.file_state = result, state
        return result


@dataclass(frozen=True)
class _TransferPlan:
    """Record a validated HTTP response's resume offset and expected transfer."""

    expected: int
    mode: Literal["ab", "wb"]
    restart: bool
    etag: str | None
    last_modified: str | None


def _plan_transfer(
    response: HTTPResponse, position: int, identity: SnapshotIdentity, validator: str | None, *, had_etag: bool
) -> _TransferPlan:
    """Check one response against the recorded validators and derive the write plan."""
    encoding = response.headers.get("Content-Encoding", "identity")
    if encoding != "identity":
        raise ValueError(f"Unexpected HTTP content encoding: {encoding}")
    etag = response.headers.get("ETag")
    strong_etag = etag if etag and not etag.startswith("W/") else None
    last_modified = _valid_http_date(response.headers.get("Last-Modified"))
    if validator and position:
        new_validator = strong_etag if had_etag else last_modified
        if new_validator and new_validator != validator:
            raise ValueError("Remote HTTP validator conflicts with partial download")
    expected: int
    mode: Literal["ab", "wb"]
    restart: bool
    if response.status == 206:
        match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", ""))
        if not match:
            raise ValueError("Malformed HTTP Content-Range")
        start, end, total = map(int, match.groups())
        if start != position or total != identity.size or end < start or end >= total:
            raise ValueError("Inconsistent HTTP Content-Range")
        expected, mode, restart = end - start + 1, "ab", False
    elif response.status == 200:
        expected, mode, restart = identity.size, "wb", True
    else:
        raise ValueError(f"Unexpected HTTP status {response.status}")
    length = response.headers.get("Content-Length")
    if length is not None and int(length) != expected:
        raise ValueError("HTTP Content-Length disagrees with archive/range")
    return _TransferPlan(expected, mode, restart, strong_etag, last_modified)


class ArchiveDownloader:
    def __init__(
        self,
        client: TransferClient,
        url_factory: Callable[[SnapshotIdentity], str] = ArchiveSource.download_url,
        attempts: int = 3,
        buffer_bytes: int = 1024**2,
        disk_reserve: int = 256 * 1024**2,
    ) -> None:
        """Set transfer dependencies, retry limits, buffer size, and disk reserve."""
        if min(attempts, buffer_bytes) <= 0:
            raise ValueError("Transfer attempts and buffer must be positive")
        self.client, self.url_factory = client, url_factory
        self.attempts, self.buffer_bytes, self.disk_reserve = attempts, buffer_bytes, disk_reserve

    def download(self, identity: SnapshotIdentity, paths: SnapshotPaths) -> ArchiveChecksum:
        """Resume an HTTP archive transfer, verify it, and publish it atomically."""
        paths.archive_dir.mkdir(parents=True, exist_ok=True)
        destination = paths.archive
        if destination.exists():
            return verify_archive(destination, identity, self.buffer_bytes)
        partial = destination.with_suffix(".7z.part")
        state_store = ManifestStore(paths.archive_dir / "transfer.json")
        state = state_store.read()
        if state and state.get("identity") != identity.digest:
            raise ValueError("Partial download identity mismatch")
        if partial.exists() and (partial.is_symlink() or not state):
            raise ValueError("Partial file lacks recorded identity or is unsafe")
        state["identity"] = identity.digest
        state_store.write(state)
        if partial.exists() and partial.stat().st_size > identity.size:
            raise ValueError("Partial download exceeds published archive size")
        hashes = _hashes(identity)
        position = 0
        if partial.exists():
            LOGGER.info("Hash existing partial archive before resuming: %s", format_bytes(partial.stat().st_size))
            partial_size = partial.stat().st_size
            _digest_file(partial, hashes, label="Resume checksum", total=partial_size, buffer_bytes=self.buffer_bytes)
            position = partial_size
        LOGGER.info(
            "Download %s: %s total; resume at %s",
            identity.item_id,
            format_bytes(identity.size),
            format_bytes(position),
        )
        with ProgressReporter(LOGGER, "Download", initial=position, total=identity.size).track() as progress:
            try:
                for attempt in range(self.attempts):
                    if position == identity.size:
                        break
                    require_disk(paths.archive_dir, identity.size - position, self.disk_reserve)
                    headers = {"Accept-Encoding": "identity"}
                    stored_etag = state.get("etag")
                    stored_modified = state.get("last_modified")
                    validator = (
                        json_string(stored_etag)
                        if stored_etag
                        else _valid_http_date(json_string(stored_modified) if stored_modified else None)
                    )
                    if position:
                        headers["Range"] = f"bytes={position}-"
                        if validator:
                            headers["If-Range"] = validator
                    try:
                        response = self.client.open(self.url_factory(identity), headers)
                    except HTTPError as error:
                        if error.code == 416:
                            error.close()
                            if position == identity.size:
                                break
                            raise ValueError("416 with incomplete local archive") from error
                        if error.code < 500 or attempt + 1 == self.attempts:
                            raise
                        LOGGER.warning(
                            "HTTP %s; retry download %s/%s at byte %s",
                            error.code,
                            attempt + 2,
                            self.attempts,
                            position,
                        )
                        time.sleep(archive_http_delay(attempt))
                        continue
                    except OSError as error:
                        if attempt + 1 == self.attempts:
                            raise OSError(f"Download connection failed for {identity.item_id}: {error}") from error
                        LOGGER.warning(
                            "Connection failed; retry download %s/%s at byte %s",
                            attempt + 2,
                            self.attempts,
                            position,
                        )
                        time.sleep(archive_http_delay(attempt))
                        continue
                    try:
                        with response:
                            plan = _plan_transfer(
                                response, position, identity, validator, had_etag=bool(state.get("etag"))
                            )
                            if plan.restart:
                                if position:
                                    LOGGER.warning("Server returned full content; restart download from byte zero")
                                position, hashes = 0, _hashes(identity)
                                progress.restart()
                            state.update(etag=plan.etag, last_modified=plan.last_modified)
                            state_store.write(state)
                            received = 0
                            with partial.open(plan.mode) as stream:
                                try:
                                    while chunk := response.read(min(self.buffer_bytes, plan.expected - received + 1)):
                                        if received + len(chunk) > plan.expected:
                                            raise ValueError("HTTP body exceeds advertised range")
                                        require_disk(paths.archive_dir, len(chunk), self.disk_reserve)
                                        stream.write(chunk)
                                        for digest in hashes.values():
                                            digest.update(chunk)
                                        received += len(chunk)
                                        position += len(chunk)
                                        progress.update(position, identity.size)
                                finally:
                                    stream.flush()
                                    os.fsync(stream.fileno())
                            if received != plan.expected:
                                raise OSError(f"Short HTTP body: received {received}, expected {plan.expected}")
                    except (OSError, IncompleteRead) as error:
                        if attempt + 1 == self.attempts:
                            raise OSError(
                                f"Download interrupted for {identity.item_id} at byte {position}: {error}",
                            ) from error
                        LOGGER.warning(
                            "Transfer interrupted; retry %s/%s from byte %s",
                            attempt + 2,
                            self.attempts,
                            position,
                        )
                        time.sleep(archive_http_delay(attempt))
                if position != identity.size:
                    raise OSError(f"Download exhausted retries at {position}/{identity.size}")
                for algorithm, expected_digest in identity.checksums:
                    if hashes[algorithm].hexdigest() != expected_digest:
                        raise ValueError(f"Archive {algorithm} checksum mismatch")
                result = ArchiveChecksum(position, hashes["sha256"].hexdigest())
                os.replace(partial, destination)
                fsync_directory(destination.parent)
                state.update(complete=True, fingerprint=result.to_json())
                state_store.write(state)
                progress.update(position, identity.size, force=True)
                LOGGER.info("Archive verified and published: %s", destination)
                stage_event("archive_verified", archive_bytes=position, snapshot=identity.item_id)
                return result
            except ValueError:
                if partial.exists():
                    os.replace(partial, partial.with_suffix(".invalid"))
                    fsync_directory(partial.parent)
                state_store.write({"identity": identity.digest, "invalid": True})
                raise
