"""Download isolated Academic Torrents files with resume and integrity checks."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import TypeAlias
from urllib.parse import quote

from sql.archive.download import verify_archive
from sql.archive.source import MetadataClient, TransferClient, safe_member_path
from sql.domain.models import ArchiveChecksum, SnapshotIdentity
from utils.artifacts import fsync_directory
from utils.config import SnapshotPaths
from utils.logging_utils import ProgressReporter, get_logger, stage_event
from utils.manifest import ManifestStore
from utils.resources import require_disk
from utils.types import json_array, json_object, json_string

LOGGER = get_logger(__name__)
Bencode: TypeAlias = int | bytes | list["Bencode"] | dict[bytes, "Bencode"]
_MAX_TORRENT_BYTES = 8 * 1024**2
_READOUT = re.compile(rb"\[#[0-9a-f]+ (\d+)B/(\d+)B\(")
_CHECKSUM_READOUT = re.compile(rb"\[Checksum:#[0-9a-f]+ (\d+)B/(\d+)B\(")


class TorrentUnavailableError(RuntimeError):
    """Report an unverified torrent transfer; HTTP may still be available."""


ARIA2C_UNAVAILABLE = "aria2c is unavailable; install it separately to enable torrents"
TORRENT_STAGING_SYMLINK = "Torrent staging directory must not be a symlink"


def aria2c_executable(binary: str | None) -> str | None:
    """Resolve the default aria2c path for preflight and downloading."""
    return shutil.which(binary or "aria2c")


@dataclass
class _Decoder:
    body: bytes
    position: int = 0
    nodes: int = 0

    def value(self, depth: int = 0) -> Bencode:
        """Decode one bounded bencode value and advance the byte position."""
        self.nodes += 1
        if depth > 32 or self.nodes > 100_000 or self.position >= len(self.body):
            raise ValueError("Torrent metadata exceeds structure budget or is truncated")
        token = self.body[self.position : self.position + 1]
        if token == b"i":
            end = self.body.index(b"e", self.position)
            number = self.body[self.position + 1 : end]
            if not re.fullmatch(rb"0|-?[1-9][0-9]*", number):
                raise ValueError("Invalid torrent integer")
            self.position = end + 1
            return int(number)
        if token in (b"l", b"d"):
            self.position += 1
            values: list[Bencode] = []
            mapping: dict[bytes, Bencode] = {}
            while self.position < len(self.body) and self.body[self.position : self.position + 1] != b"e":
                child = self.value(depth + 1)
                if token == b"l":
                    values.append(child)
                else:
                    if not isinstance(child, bytes) or child in mapping:
                        raise ValueError("Invalid or duplicate torrent dictionary key")
                    mapping[child] = self.value(depth + 1)
            if self.position >= len(self.body):
                raise ValueError("Unterminated torrent container")
            self.position += 1
            return values if token == b"l" else mapping
        end = self.body.index(b":", self.position)
        number = self.body[self.position : end]
        if not re.fullmatch(rb"0|[1-9][0-9]*", number):
            raise ValueError("Invalid torrent string length")
        length = int(number)
        self.position = end + 1 + length
        if self.position > len(self.body):
            raise ValueError("Truncated torrent string")
        return self.body[end + 1 : self.position]


def _mapping(value: Bencode) -> dict[bytes, Bencode]:
    """Require a bencode dictionary or raise ValueError."""
    if not isinstance(value, dict):
        raise ValueError("Expected torrent dictionary")
    return value


def _text(value: Bencode) -> str:
    """Decode a bencode byte string as UTF-8 text."""
    if not isinstance(value, bytes):
        raise ValueError("Expected torrent string")
    return value.decode("utf-8")


@dataclass(frozen=True)
class TorrentSelection:
    index: int
    file_count: int
    transfer_size: int


def select_torrent(body: bytes, identity: SnapshotIdentity) -> TorrentSelection:
    """Select the exact published path and size; reject unsafe torrent members."""
    if len(body) > _MAX_TORRENT_BYTES:
        raise ValueError("Torrent exceeds metadata budget")
    decoder = _Decoder(body)
    torrent = _mapping(decoder.value())
    if decoder.position != len(body):
        raise ValueError("Trailing torrent metadata")
    info = _mapping(torrent[b"info"])
    name = _text(info[b"name"])
    safe_member_path(name)
    if "/" in name:
        raise ValueError("Torrent name must be one path component")
    if b"name.utf-8" in info and _text(info[b"name.utf-8"]) != name:
        raise ValueError("Conflicting torrent names")
    files = info.get(b"files")
    if files is None:
        files = [{b"length": info[b"length"], b"path": [info[b"name"]]}]
    if not isinstance(files, list) or not files:
        raise ValueError("Expected torrent file list")
    selected: list[int] = []
    selected_offset = 0
    total_length = 0
    seen: set[str] = set()
    for index, entry in enumerate(files, start=1):
        member = _mapping(entry)
        parts = member[b"path"]
        if not isinstance(parts, list) or not parts:
            raise ValueError("Expected torrent member path")
        if b"path.utf-8" in member and member[b"path.utf-8"] != parts:
            raise ValueError("Conflicting torrent paths")
        path_parts = [_text(part) for part in parts]
        if any("/" in part for part in path_parts):
            raise ValueError("Torrent path component contains separator")
        path = safe_member_path("/".join(path_parts)).as_posix()
        attributes = member.get(b"attr", b"")
        if not isinstance(attributes, bytes):
            raise ValueError("Invalid torrent member attributes")
        if path in seen or b"symlink path" in member or b"l" in attributes:
            raise ValueError("Duplicate or linked torrent member")
        seen.add(path)
        size = member[b"length"]
        if not isinstance(size, int) or size < 0:
            raise ValueError("Invalid torrent member size")
        if identity.archive_name in (path, f"{name}/{path}"):
            if size != identity.size:
                raise ValueError("Torrent archive size conflicts with published identity")
            selected.append(index)
            selected_offset = total_length
        total_length += size
    if len(selected) != 1:
        raise ValueError("Torrent must contain exactly the published archive path")
    piece_length = info.get(b"piece length")
    if not isinstance(piece_length, int) or piece_length <= 0:
        raise ValueError("Invalid torrent piece length")
    # aria2 counts whole pieces intersecting selected files, including boundary bytes.
    first_piece = selected_offset // piece_length
    last_piece_end = ((selected_offset + identity.size + piece_length - 1) // piece_length) * piece_length
    transfer_size = min(total_length, last_piece_end) - first_piece * piece_length if identity.size else 0
    return TorrentSelection(selected[0], len(files), transfer_size)


def torrent_remaining_bytes(identity: SnapshotIdentity, paths: SnapshotPaths) -> int:
    """Budget resume using allocated blocks, never a sparse file's logical size."""
    work = paths.archive_dir / "torrent"
    target = work / "archive.part"
    if work.is_symlink() or target.is_symlink():
        raise ValueError("Torrent staging must not contain symlinks")
    state = ManifestStore(work / "transfer.json").read()
    if state and state.get("identity") != identity.digest:
        raise ValueError("Torrent partial download identity mismatch")
    if not target.exists():
        return identity.size
    if not state or not target.is_file():
        raise ValueError("Torrent partial lacks recorded identity or is unsafe")
    return max(0, identity.size - target.stat().st_blocks * 512)


@dataclass(frozen=True)
class _TorrentJob:
    work: Path
    target: Path
    command: list[str]
    transfer_size: int


class TorrentDownloader:
    def __init__(
        self,
        client: TransferClient,
        metadata: MetadataClient,
        *,
        binary: str | None = None,
        disk_reserve: int = 256 * 1024**2,
        idle_timeout: int = 180,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Set torrent clients, disk reserve, binary path, and stall timeout."""
        if idle_timeout <= 0:
            raise ValueError("Torrent idle timeout must be positive")
        self.client, self.metadata, self.binary = client, metadata, binary
        self.disk_reserve, self.idle_timeout, self.clock = disk_reserve, idle_timeout, clock

    def _torrent(self, identity: SnapshotIdentity) -> bytes:
        """Fetch bounded torrent metadata for the matching archive item."""
        metadata = self.metadata.json(f"https://archive.org/metadata/{quote(identity.item_id, safe='')}")
        if json_object(metadata.get("metadata", {})).get("identifier") != identity.item_id:
            raise ValueError("Torrent metadata item identity mismatch")
        names = [
            json_string(member["name"])
            for entry in json_array(metadata.get("files", []))
            if (member := json_object(entry)).get("format") in ("BitTorrent", "Archive BitTorrent")
        ]
        if len(names) != 1:
            raise ValueError("Expected exactly one BitTorrent file")
        safe_member_path(names[0])
        url = f"https://archive.org/download/{quote(identity.item_id, safe='')}/{quote(names[0], safe='/')}"
        with self.client.open(url) as response:
            if response.status != 200:
                raise ValueError("Unexpected torrent metadata response")
            body = response.read(_MAX_TORRENT_BYTES + 1)
        if len(body) > _MAX_TORRENT_BYTES:
            raise ValueError("Torrent exceeds metadata budget")
        return body

    def _prepare(self, identity: SnapshotIdentity, paths: SnapshotPaths) -> _TorrentJob:
        """Validate torrent ownership, reserve disk, and build the aria2c job."""
        binary = aria2c_executable(self.binary)
        if binary is None:
            raise TorrentUnavailableError(ARIA2C_UNAVAILABLE)
        work = paths.archive_dir / "torrent"
        if work.is_symlink():
            raise ValueError(TORRENT_STAGING_SYMLINK)
        work.mkdir(parents=True, exist_ok=True)
        state = ManifestStore(work / "transfer.json")
        previous = state.read()
        if previous and previous.get("identity") != identity.digest:
            raise ValueError("Torrent partial download identity mismatch")
        if not previous and any(work.iterdir()):
            raise ValueError("Torrent staging lacks recorded identity")
        state.write({"identity": identity.digest})
        target = work / "archive.part"
        for member in work.iterdir():
            if member.is_symlink() or not member.is_file():
                raise ValueError("Unsafe torrent staging member")
        try:
            body = self._torrent(identity)
            selection = select_torrent(body, identity)
        except (OSError, ValueError, KeyError) as error:
            raise TorrentUnavailableError(f"Torrent metadata unavailable or unsuitable: {error}") from error
        # Resume only the same torrent; control files cannot describe another piece layout.
        digest = sha256(body).hexdigest()
        if previous.get("torrent_sha256") not in (None, digest):
            raise TorrentUnavailableError("Torrent metadata changed; refusing incompatible resume state")
        state.write({"identity": identity.digest, "torrent_sha256": digest})
        torrent_path = work / "source.torrent"
        torrent_path.write_bytes(body)
        require_disk(work, torrent_remaining_bytes(identity, paths), self.disk_reserve)
        command = [
            binary,
            "--no-conf=true",
            "--no-netrc=true",
            f"--dir={work}",
            f"--select-file={selection.index}",
            "--seed-time=0",
            "--check-integrity=true",
            "--file-allocation=none",
            "--auto-file-renaming=false",
            "--allow-overwrite=true",
            "--enable-color=false",
            "--human-readable=false",
            "--summary-interval=10",
            "--truncate-console-readout=false",
            "--console-log-level=notice",
            "--download-result=hide",
            "--enable-rpc=false",
            "--bt-save-metadata=false",
            "--disk-cache=16M",
            "--bt-max-peers=32",
            "--bt-tracker=https://academictorrents.com/announce.php",
            f"--dht-file-path={work / 'dht.dat'}",
            f"--dht-file-path6={work / 'dht6.dat'}",
            "--max-concurrent-downloads=1",
            f"--bt-stop-timeout={self.idle_timeout}",
            *(
                f"--index-out={index}={'archive.part' if index == selection.index else f'unselected-{index}.part'}"
                for index in range(1, selection.file_count + 1)
            ),
            str(torrent_path),
        ]
        return _TorrentJob(work, target, command, selection.transfer_size)

    def download(self, identity: SnapshotIdentity, paths: SnapshotPaths) -> ArchiveChecksum:
        """Download and verify a torrent archive, then publish the checked file."""
        if paths.archive.exists():
            return verify_archive(paths.archive, identity)
        job = self._prepare(identity, paths)
        work, target, command = job.work, job.target, job.command
        LOGGER.info(
            "Download via Academic Torrents swarm with aria2c; stall timeout %ss; diagnostics: %s",
            self.idle_timeout,
            work / "aria2.log",
        )
        try:
            self._run(command, work, job.transfer_size, resuming=target.exists())
            result = verify_archive(target, identity)
        except (OSError, RuntimeError, ValueError) as error:
            raise TorrentUnavailableError(f"Torrent transfer failed: {error}") from error
        with target.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(target, paths.archive)
        fsync_directory(paths.archive_dir)
        self.discard(paths)
        LOGGER.info("Torrent archive verified and published: %s", paths.archive)
        stage_event("archive_verified", archive_bytes=identity.size, snapshot=identity.item_id, method="torrent")
        return result

    def _run(
        self,
        command: list[str],
        work: Path,
        transfer_size: int,
        *,
        resuming: bool,
    ) -> None:
        """Run aria2c with progress and stall detection, stopping it on exit."""
        started = self.clock()
        with (work / "aria2.log").open("wb") as output, (work / "aria2.log").open("rb") as reader:
            process = subprocess.Popen(
                command, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT, cwd=work
            )
            try:
                with ProgressReporter(LOGGER, "Torrent download", total=transfer_size).track() as progress:
                    completed = 0
                    last_change = started
                    pending = b""
                    checked = 0
                    while True:
                        try:
                            process.wait(timeout=0.2)
                            done = True
                        except subprocess.TimeoutExpired:
                            done = False
                        if output.tell() > 16 * 1024**2:
                            raise RuntimeError("Torrent diagnostics exceed budget")
                        pending = (pending + reader.read(64 * 1024))[-128 * 1024 :]
                        checks = list(_CHECKSUM_READOUT.finditer(pending))
                        if checks and int(checks[-1][1]) > checked:
                            checked, last_change = int(checks[-1][1]), self.clock()
                        matches = list(_READOUT.finditer(pending))
                        if matches:
                            count, total = map(int, matches[-1].groups())
                            if total != transfer_size or count > total:
                                raise ValueError(
                                    f"Torrent progress total {total} conflicts with expected piece span {transfer_size}, or count {count} exceeds total"
                                )
                            if count > completed:
                                completed, last_change = count, self.clock()
                            if resuming and count:
                                progress.initial = count
                                progress.started = self.clock()
                                resuming = False
                            progress.update(count, total)
                            pending = pending[matches[-1].end() :]
                        require_disk(work, 0, self.disk_reserve)
                        if done:
                            if process.returncode:
                                raise RuntimeError(
                                    f"aria2c exited with status {process.returncode}; see {work / 'aria2.log'}"
                                )
                            progress.update(transfer_size, force=True)
                            return
                        if self.clock() - last_change > self.idle_timeout:
                            raise RuntimeError("Torrent made no progress before stall timeout")
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()

    @staticmethod
    def discard(paths: SnapshotPaths) -> None:
        """Remove only owned scratch files, never HTTP partials or published data."""
        work = paths.archive_dir / "torrent"
        if not work.exists():
            return
        if work.is_symlink():
            raise ValueError("Unsafe torrent staging directory")
        state = ManifestStore(work / "transfer.json").read()
        if not state.get("identity"):
            raise ValueError("Torrent scratch files lack recorded identity")
        for path in work.iterdir():
            if (
                path.is_symlink()
                or not path.is_file()
                or not re.fullmatch(
                    r"archive\.part|unselected-\d+\.part|.+\.aria2|source\.torrent|transfer\.json|aria2\.log|dht6?\.dat(?:\.tmp)?",
                    path.name,
                )
            ):
                raise ValueError("Unexpected torrent scratch file")
        for path in work.iterdir():
            # Preserve bounded diagnostics after a failed torrent attempt.
            if path.name != "aria2.log":
                path.unlink()
        ManifestStore(work / "transfer.json").write({"identity": state["identity"]})
