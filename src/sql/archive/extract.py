"""Inspect native 7-Zip archives and atomically publish selected XML files."""

import os
import re
import selectors
import shutil
import subprocess
import tempfile
import time
from collections.abc import Sized
from pathlib import Path
from typing import TypedDict, cast

from sql.archive.source import safe_member_path
from sql.domain.models import ArchiveChecksum, BinaryEvidence, SnapshotIdentity, XMLInput, XMLInventory
from sql.domain.schema import TABLES
from utils.artifacts import fsync_directory
from utils.config import SnapshotPaths
from utils.logging_utils import ProgressReporter, format_bytes, get_logger, stage_event
from utils.manifest import ManifestStore
from utils.resources import ResourceBudget, ResourcePlan, require_disk, require_memory, worker_capacity
from utils.types import JSONObject, json_integer, json_object, json_string

LOGGER = get_logger(__name__)

MINIMUM_NATIVE_VERSION = (26, 4)


class ArchiveMember(TypedDict):
    name: str
    size: int
    directory: bool
    method: str


ArchiveListing = tuple[tuple[ArchiveMember, ...], dict[str, ArchiveMember], int]


def member_json(member: ArchiveMember) -> JSONObject:
    """Copy archive member metadata into JSON fields."""
    return {
        "name": member["name"],
        "size": member["size"],
        "directory": member["directory"],
        "method": member["method"],
    }


def binary_json(binary: BinaryEvidence) -> JSONObject:
    """Serialize the checked binary path and version."""
    return {"path": binary["path"], "version": binary["version"]}


def _check_candidate(selected: str) -> BinaryEvidence:
    """Require a runnable native 7-Zip version 26.04 or newer."""
    if not selected or not Path(selected).is_file():
        raise RuntimeError("Native 7-Zip 26.04+ required; binary is missing")
    selected = str(Path(selected).resolve())
    try:
        result = subprocess.run([selected, "i"], capture_output=True, text=True, timeout=10, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RuntimeError(f"Native 7-Zip 26.04+ required; cannot run {selected}: {error}") from error
    match = re.search(r"^7-Zip (?:\(z\) )?(\d+)\.(\d+)\b", result.stdout, re.M)
    if (
        result.returncode
        or "p7zip" in (result.stdout + result.stderr).lower()
        or not match
        or tuple(map(int, match.groups())) < MINIMUM_NATIVE_VERSION
    ):
        raise RuntimeError("Native 7-Zip 26.04+ required; p7zip and older versions are rejected")
    return {"path": selected, "version": ".".join(match.groups())}


def checked_binary(binary: str | None = None) -> BinaryEvidence:
    """Find and validate an explicit or PATH-provided native 7-Zip binary."""
    if binary is not None:
        return _check_candidate(shutil.which(binary) or binary)
    failures: list[str] = []
    for name in ("7zz", "7z"):
        candidate = shutil.which(name)
        if candidate:
            try:
                return _check_candidate(candidate)
            except (RuntimeError, ValueError, OSError) as error:
                failures.append(f"{candidate}: {error}")
    detail = "; ".join(failures) or "no 7zz or 7z found in PATH"
    raise RuntimeError(f"Native 7-Zip 26.04+ required; {detail}. No automatic installation.")


def inspect_members(output: str) -> ArchiveListing:
    """Parse a 7z listing and validate selected XML paths and metadata."""
    if "\n----------\n" not in output:
        raise ValueError("Invalid 7-Zip technical listing")
    header, body = output.split("\n----------\n", 1)
    if not re.search(r"^Type = 7z$", header, re.M):
        raise ValueError("Expected a native 7z archive")
    members: list[ArchiveMember] = []
    selected: dict[str, ArchiveMember] = {}
    seen: set[str] = set()
    dictionary = 0
    for block in cast(list[str], re.split(r"\n\s*\n", body.strip())):
        if not block.strip():
            continue
        fields: dict[str, str] = {}
        for line in block.splitlines():
            if " = " not in line:
                raise ValueError("Invalid archive member metadata")
            key, value = line.split(" = ", 1)
            if key in fields:
                raise ValueError("Duplicate archive metadata key")
            fields[key] = value
        name = fields.get("Path", "")
        path = safe_member_path(name)
        if name in seen:
            raise ValueError(f"Duplicate archive member: {name}")
        seen.add(name)
        attributes = fields.get("Attributes", "")
        if any("link" in key.lower() for key in fields) or "L" in attributes or re.search(r"\bl[rwx-]{9}", attributes):
            raise ValueError(f"Links are forbidden: {name}")
        directory = (
            fields.get("Folder") == "+" or attributes.startswith("D") or bool(re.search(r"\bd[rwx-]{9}", attributes))
        )
        if fields.get("Encrypted", "-") != "-":
            raise ValueError("Encrypted dump members are unsupported")
        size = int(fields.get("Size", "0"))
        if size < 0:
            raise ValueError("Negative unpacked member size")
        method = fields.get("Method", "")
        if not directory:
            if not re.fullmatch(r"(?:LZMA2?:\d+|Copy)(?: (?:BCJ|BCJ2|Delta:\d+))*", method):
                raise ValueError(f"Unsupported archive method: {method}")
            for dictionary_match in re.finditer(r"LZMA2?:(\d+)", method):
                exponent = int(dictionary_match[1])
                if not 1 <= exponent <= 40:
                    raise ValueError("Unsupported archive dictionary exponent")
                dictionary = max(dictionary, 1 << exponent)
        member: ArchiveMember = {"name": name, "size": size, "directory": directory, "method": method}
        members.append(member)
        matches = [
            table
            for table in TABLES
            if path.name.lower().startswith(table.lower()) and path.name.lower().endswith(".xml")
        ]
        for table in matches:
            if path.name != f"{table}.xml" or directory or table in selected:
                raise ValueError(f"Ambiguous or unsupported selected member: {name}")
            selected[table] = member
    if set(selected) != set(TABLES):
        raise ValueError("Missing selected XML members: " + ", ".join(sorted(set(TABLES) - set(selected))))
    return tuple(members), selected, dictionary


def read_inventory(paths: SnapshotPaths, identity: SnapshotIdentity) -> XMLInventory:
    """Read matching XML inventory metadata and verify output file sizes."""
    state = ManifestStore(paths.xml / "inventory.json").read()
    selected = json_object(state.get("selected", {}))
    if not state or state.get("identity") != identity.digest or set(selected) != set(TABLES):
        raise ValueError("XML inventory lacks matching identity and all selected tables")
    inputs: list[XMLInput] = []
    for table in TABLES:
        value = json_object(selected[table])
        relative = safe_member_path(json_string(value["name"]))
        path = paths.xml / relative
        if path.resolve() != path or path.is_symlink():
            raise ValueError(f"Unsafe XML output: {path}")
        size = json_integer(value["size"])
        if not path.is_file() or path.stat().st_size != size:
            raise ValueError(f"XML size changed: {table}")
        inputs.append(XMLInput(table, path, size))
    return XMLInventory(tuple(inputs), ArchiveChecksum.from_dict(json_object(state["archive_fingerprint"])))


class SevenZipExtractor:
    def __init__(self, binary: str | None = None) -> None:
        """Store an optional binary path without launching 7-Zip."""
        self.binary = binary
        self.binary_evidence: BinaryEvidence | None = None

    def preflight(self) -> BinaryEvidence:
        """Find a supported native 7-Zip binary and retain its version evidence."""
        if self.binary_evidence is not None:
            path = Path(self.binary_evidence["path"])
            if path.is_file() and not path.is_symlink():
                return self.binary_evidence
        self.binary_evidence = checked_binary(self.binary)
        LOGGER.info("Native 7-Zip %s: %s", self.binary_evidence["version"], self.binary_evidence["path"])
        return self.binary_evidence

    def list_archive(self, archive: Path) -> ArchiveListing:
        """Read a bounded 7-Zip member listing with a timeout."""
        self.binary_evidence = self.preflight()
        command = [self.binary_evidence["path"], "l", "-slt", "-sccUTF-8", str(archive)]
        output = bytearray()
        budget = 16 * 1024**2
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if process.stdout is None:
            raise RuntimeError("7-Zip listing output pipe unavailable")
        try:
            deadline = time.monotonic() + 60
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while True:
                    if time.monotonic() > deadline:
                        raise RuntimeError("7-Zip listing timed out")
                    if not cast(Sized, selector.select(timeout=0.2)):
                        continue
                    chunk = os.read(process.stdout.fileno(), min(64 * 1024, budget - len(output) + 1))
                    if not chunk:
                        break
                    if len(output) + len(chunk) > budget:
                        raise ValueError("Archive inventory exceeds metadata budget")
                    output.extend(chunk)
            process.wait(timeout=max(0.1, deadline - time.monotonic()))
            if process.returncode:
                raise RuntimeError(f"7-Zip listing failed: {output[-1000:].decode('utf-8', 'replace')}")
            return inspect_members(output.decode("utf-8", "strict"))
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            process.stdout.close()

    def extract(
        self,
        identity: SnapshotIdentity,
        paths: SnapshotPaths,
        resources: ResourcePlan,
        archive_checksum: ArchiveChecksum,
    ) -> XMLInventory:
        """Extract selected XML files within budgets and publish their inventory."""
        if paths.xml.exists():
            return read_inventory(paths, identity)
        members, selected, dictionary = self.list_archive(paths.archive)
        if self.binary_evidence is None:
            raise RuntimeError("7-Zip binary evidence unavailable")
        LOGGER.info(
            "Archive inventory: %s members; %s selected XML tables; dictionary %s",
            len(members),
            len(selected),
            format_bytes(dictionary),
        )
        threads = (
            worker_capacity(
                ResourceBudget(resources.threads, resources.memory_bytes, resources.evidence),
                dictionary,
                reserved_memory_bytes=64 * 1024**2,
            )
            if dictionary
            else resources.threads
        )
        if threads < 1:
            raise RuntimeError(f"Archive dictionary exceeds extraction RAM budget: {dictionary} bytes")
        require_memory(dictionary + 64 * 1024**2)
        paths.xml.parent.mkdir(parents=True, exist_ok=True)
        unpacked = sum(value["size"] for value in selected.values())
        require_disk(paths.xml.parent, unpacked, resources.disk_reserve)
        staging = Path(tempfile.mkdtemp(prefix=f".{paths.item_id}-xml-", dir=paths.xml.parent))
        paths.state.mkdir(parents=True, exist_ok=True)
        log_path = paths.state / "extract.log"
        command = [
            self.binary_evidence["path"],
            "x",
            "-y",
            "-spd",
            "-sccUTF-8",
            f"-mmt={threads}",
            f"-o{staging}",
            str(paths.archive),
            *(value["name"] for value in selected.values()),
        ]
        LOGGER.info(
            "Extract with native 7-Zip %s; %s threads; %s selected XML bytes; diagnostics: %s",
            self.binary_evidence["version"],
            threads,
            format_bytes(unpacked),
            log_path,
        )
        with ProgressReporter(LOGGER, "Extraction", total=unpacked).track() as progress:
            try:
                with log_path.open("wb") as log:
                    process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
                    try:
                        while True:
                            try:
                                process.wait(timeout=0.2)
                                break
                            except subprocess.TimeoutExpired:
                                require_disk(staging, 0, resources.disk_reserve)
                                require_memory(64 * 1024**2)
                                try:
                                    status = Path(f"/proc/{process.pid}/status").read_text()
                                    rss = re.search(r"^VmRSS:\s+(\d+) kB", status, re.M)
                                    if rss and int(rss[1]) * 1024 > resources.memory_bytes:
                                        raise RuntimeError("7-Zip process exceeds configured RAM budget")
                                except FileNotFoundError:
                                    pass
                                if log.tell() > 16 * 1024**2:
                                    raise RuntimeError("7-Zip diagnostic output exceeds budget") from None
                                written = sum(
                                    path.stat().st_size
                                    for value in selected.values()
                                    if (path := staging / value["name"]).is_file()
                                )
                                progress.update(written, unpacked)
                    except BaseException:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                        raise
                with log_path.open("rb") as log:
                    log.seek(max(0, log_path.stat().st_size - 4096))
                    tail = log.read().decode("utf-8", "replace")
                LOGGER.debug("7-Zip diagnostics:\n%s", tail.rstrip())
                if process.returncode or "Everything is Ok" not in tail:
                    raise RuntimeError(f"7-Zip extraction failed (exit {process.returncode}): {tail[-1000:]}")
                output: JSONObject = {}
                for table, value in selected.items():
                    path = staging / value["name"]
                    if (
                        path.resolve() != path
                        or path.is_symlink()
                        or not path.is_file()
                        or path.stat().st_size != value["size"]
                    ):
                        raise ValueError(f"Extracted member type/size mismatch: {table}")
                    with path.open("rb") as stream:
                        os.fsync(stream.fileno())
                    output[table] = member_json(value)
                allowed = {value["name"] for value in selected.values()}
                for path in staging.rglob("*"):
                    if path.is_symlink() or (path.is_file() and path.relative_to(staging).as_posix() not in allowed):
                        raise ValueError(f"Unexpected extracted member: {path.name}")
                ManifestStore(staging / "inventory.json").write(
                    {
                        "identity": identity.digest,
                        "members": [member_json(member) for member in members],
                        "selected": output,
                        "excluded": [member_json(member) for member in members if member["name"] not in allowed],
                        "archive_fingerprint": archive_checksum.to_json(),
                        "binary": binary_json(self.binary_evidence),
                        "extraction_resources": {
                            "threads": threads,
                            "dictionary_bytes": dictionary,
                            "memory_bytes": resources.memory_bytes,
                        },
                    },
                )
                for directory in sorted((x for x in staging.rglob("*") if x.is_dir()), reverse=True):
                    fsync_directory(directory)
                fsync_directory(staging)
                os.replace(staging, paths.xml)
                fsync_directory(paths.xml.parent)
                progress.update(unpacked, unpacked, force=True)
                LOGGER.info("Verified XML inventory published: %s", paths.xml)
                stage_event("xml_published", xml_bytes=unpacked, tables=list(selected), xml_path=paths.xml)
                return read_inventory(paths, identity)
            finally:
                if staging.exists():
                    shutil.rmtree(staging)
