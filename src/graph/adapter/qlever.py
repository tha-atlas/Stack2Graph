"""Run a fixed QLever version and control only owned candidate containers."""

import json
import os
import re
import shlex
import subprocess
import sys
import time
from hashlib import sha256
from pathlib import Path
from shutil import copyfile
from threading import Event, Timer

from graph.adapter.catalog_stream import stream_catalog
from graph.adapter.inventory import file_ref, verify_file
from graph.adapter.staging import IndexInputStager
from graph.domain.artifacts import ArtifactLayout, FileRef
from graph.domain.index import IndexManifest, QLeverSettings, SelectedArtifacts
from utils.artifacts import AtomicBinaryWriter, atomic_write_bytes, atomic_write_json
from utils.types import json_integer, json_object, json_string, parse_json


def _size_ref(layout: ArtifactLayout, path: Path) -> FileRef:
    """Describe an owned QLever output without rereading the complete file."""
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"QLever output is not a regular file: {path}")
    relative = path.relative_to(layout.root).as_posix()
    return FileRef(relative, "", path.stat().st_size)


class QLeverAdapter:
    def __init__(self, settings: QLeverSettings) -> None:
        """Store runtime settings and initialize the inspected-index cache."""
        self.settings = settings
        self._inspected: tuple[Path, IndexManifest] | None = None

    def _run(self, arguments: tuple[str, ...], candidate: Path, log_name: str) -> None:
        """Run a bounded QLever command and write its combined output to a log."""
        with (candidate / log_name).open("wb") as output:
            result = subprocess.run(
                [str(self.settings.executable), *arguments],
                cwd=candidate,
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=self.settings.timeout_seconds,
                check=False,
            )
        if result.returncode != 0:
            raise RuntimeError(f"QLever {arguments[0]} failed ({result.returncode}); see {candidate / log_name}")

    def version(self) -> str:
        """Read the native QLever version or fail its executable preflight."""
        result = subprocess.run(
            [str(self.settings.executable), "--version"], capture_output=True, text=True, timeout=10, check=False
        )
        if result.returncode != 0 or not result.stdout.strip():
            raise RuntimeError("QLever executable version preflight failed")
        return result.stdout.strip()

    def build(self, inputs: SelectedArtifacts, candidate: Path, *, published_path: Path | None = None) -> IndexManifest:
        """Build an immutable native index and record its source provenance."""
        if candidate.exists():
            raise ValueError("Candidate directory already exists; indexes are immutable")
        candidate.mkdir(parents=True)
        if candidate.resolve() != candidate:
            raise ValueError("Candidate directory must be absolute and have no symlinks")
        version = self.version()
        source_layout = ArtifactLayout(inputs.source_root)
        from graph.services.validate import read_manifest

        source_manifest = read_manifest(source_layout, require_complete=True)
        catalog_root = candidate / "source-catalog"
        catalog_root.mkdir()
        copyfile(source_layout.manifest, catalog_root / "manifest.json")
        range_catalog = verify_file(source_layout, source_manifest.range_catalog)
        range_destination = catalog_root / source_manifest.range_catalog.path
        range_destination.parent.mkdir(parents=True, exist_ok=True)
        copyfile(range_catalog, range_destination)
        for marker in source_manifest.iter_ranges(source_layout):
            destination = catalog_root / marker.membership.path
            destination.parent.mkdir(parents=True, exist_ok=True)
            copyfile(verify_file(source_layout, marker.membership), destination)
        local_catalog = AtomicBinaryWriter(candidate / "inputs.jsonl")
        stager = IndexInputStager(candidate / "inputs")
        try:
            with inputs.shard_catalog.open() as stream:
                for number, line in enumerate(stream):
                    row = json_object(parse_json(line))
                    source_path = Path(json_string(row["path"]))
                    relative = f"inputs/{number:012d}.nq"
                    stager.stage_file(source_path, f"{number:012d}.nq")
                    row["path"] = relative
                    local_catalog.stream.write((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode())
            local_catalog.publish()
        finally:
            local_catalog.discard()
        identity = sha256(str(candidate).encode()).hexdigest()[:20]
        producer = "set -o pipefail; " + shlex.join(
            [
                sys.executable,
                str(Path(__file__).resolve().parents[1] / "start.py"),
                "stream",
                "--catalog",
                str((published_path or candidate) / "inputs.jsonl"),
            ]
        )
        settings = self.settings
        sorter_bytes, parser_bytes = self.input_memory(candidate / "inputs.jsonl")
        config = f"""[data]
NAME = kg
DESCRIPTION = Verified Stack Overflow named graphs
FORMAT = nq
[index]
INPUT_FILES = inputs.jsonl
CAT_INPUT_FILES = {producer}
PARALLEL_PARSING = true
STXXL_MEMORY = {sorter_bytes}B
PARSER_BUFFER_SIZE = {parser_bytes}B
SETTINGS_JSON = {{"num-triples-per-batch":10000,"parser-batch-size":1000}}
[server]
HOST_NAME = {settings.host}
PORT = {settings.port}
MEMORY_FOR_QUERIES = {settings.query_memory_bytes}B
CACHE_MAX_SIZE = {settings.query_memory_bytes // 4}B
CACHE_MAX_SIZE_SINGLE_ENTRY = {settings.query_memory_bytes // 8}B
TIMEOUT = {settings.timeout_seconds}s
[runtime]
SYSTEM = docker
IMAGE = {settings.image_digest}
INDEX_CONTAINER = stack2graph-kg-index-{identity}
SERVER_CONTAINER = stack2graph-kg-server-{identity}
"""
        atomic_write_bytes(candidate / "Qleverfile", config.encode())
        atomic_write_json(candidate / "kg.settings.json", {"num-triples-per-batch": 10000, "parser-batch-size": 1000})
        self._index(candidate, identity, (sorter_bytes, parser_bytes))
        if not (candidate / "kg.index.pso").is_file() or not (candidate / "kg.index.pos").is_file():
            raise RuntimeError(
                f"QLever did not produce required index permutations; see {candidate / 'index-command.log'}"
            )
        from graph.services.query_checks import write_expectations

        write_expectations(inputs, source_layout, source_manifest, candidate)
        files = AtomicBinaryWriter(candidate / "files.jsonl")
        layout = ArtifactLayout(candidate)
        count = 0
        try:
            for path in sorted(
                (
                    *candidate.glob("kg.*"),
                    candidate / "Qleverfile",
                    candidate / "expected-queries.json",
                    *catalog_root.rglob("*.json"),
                    *catalog_root.rglob("*.jsonl"),
                )
            ):
                if path.is_file() and ".log" not in path.name and "-log" not in path.name:
                    files.stream.write(
                        (
                            json.dumps(_size_ref(layout, path).to_json(), sort_keys=True, separators=(",", ":")) + "\n"
                        ).encode()
                    )
                    count += 1
            if count == 0:
                raise RuntimeError("QLever returned without producing index files")
            files.publish()
        finally:
            files.discard()
        manifest = IndexManifest(
            identity,
            inputs.identity,
            inputs.graph_keys,
            inputs.input_digest,
            settings.image_digest,
            version,
            file_ref(layout, files.path),
            False,
        )
        atomic_write_json(candidate / "index.json", manifest.to_json())
        return manifest

    def input_memory(self, catalog: Path) -> tuple[int, int]:
        """Reserve parser RAM for the largest record before sizing sort memory."""
        parser_bytes = 128 * 1024**2
        with catalog.open() as stream:
            for line in stream:
                parser_bytes = max(parser_bytes, json_integer(json_object(parse_json(line))["record_bytes"]))
        sorter_bytes = self.settings.index_memory_bytes - parser_bytes
        if sorter_bytes < 128 * 1024**2:
            raise ValueError(
                "Index RAM budget cannot hold the declared record buffer plus sort memory; increase INDEX_RAM_BYTES"
            )
        return sorter_bytes, parser_bytes

    def _index(self, candidate: Path, identity: str, memory: tuple[int, int] | None = None) -> None:
        """
        Stream input from host Python 3.14 and propagate either process's
        failure.
        """
        name = "stack2graph-kg-index-" + identity
        sorter_bytes, parser_bytes = memory if memory is not None else self.input_memory(candidate / "inputs.jsonl")
        command = [
            "docker",
            "run",
            "--rm",
            "--name",
            name,
            "-i",
            "--volume",
            f"{candidate}:/index",
            "--workdir",
            "/index",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--ulimit",
            "nofile=65536:65536",
            "--entrypoint",
            "qlever-index",
            self.settings.image_digest,
            "-i",
            "kg",
            "-f",
            "/dev/stdin",
            "-F",
            "nq",
            "-p",
            "true",
            "-s",
            "kg.settings.json",
            "-m",
            f"{sorter_bytes}B",
            "-b",
            f"{parser_bytes}B",
        ]
        with (candidate / "index-command.log").open("wb") as log:
            process = subprocess.Popen(
                command, cwd=candidate, stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT
            )
            timed_out = Event()

            def kill_on_timeout() -> None:
                """Mark the index operation timed out and kill its subprocess."""
                timed_out.set()
                process.kill()

            watchdog = Timer(self.settings.index_timeout_seconds, kill_on_timeout)
            watchdog.start()
            successful = False
            try:
                if process.stdin is None:
                    raise RuntimeError("Indexer stdin unavailable")
                try:
                    stream_catalog(candidate / "inputs.jsonl", process.stdin)
                except BrokenPipeError as error:
                    status = process.wait()
                    if timed_out.is_set():
                        raise TimeoutError(
                            f"QLever indexer exceeded {self.settings.index_timeout_seconds} seconds; "
                            f"see {candidate / 'index-command.log'}"
                        ) from error
                    raise RuntimeError(
                        f"QLever indexer stopped while streaming input ({status}); "
                        f"see {candidate / 'index-command.log'}"
                    ) from error
                finally:
                    process.stdin.close()
                status = process.wait(timeout=self.settings.index_timeout_seconds)
                if status != 0:
                    if timed_out.is_set():
                        raise TimeoutError(
                            f"QLever indexer exceeded {self.settings.index_timeout_seconds} seconds; "
                            f"see {candidate / 'index-command.log'}"
                        )
                    raise RuntimeError(f"QLever indexer failed ({status}); see {candidate / 'index-command.log'}")
                successful = True
            finally:
                watchdog.cancel()
                if not successful:
                    if process.poll() is None:
                        process.kill()
                    process.wait()
                    subprocess.run(
                        ["docker", "rm", "-f", name], stdout=log, stderr=subprocess.STDOUT, timeout=30, check=False
                    )

    def build_empty(self, directory: Path) -> None:
        """Index an empty, explicitly prepared catalog for initial service startup."""
        if (directory / "inputs.jsonl").stat().st_size != 0:
            raise ValueError("Bootstrap indexing requires an empty input catalog")
        self._index(directory, sha256(str(directory).encode()).hexdigest()[:20])
        if not (directory / "kg.index.pso").is_file() or not (directory / "kg.index.pos").is_file():
            raise RuntimeError("Empty QLever index did not produce native permutations")

    def ensure_network(self) -> None:
        """Create the configured Docker bridge network if it is absent."""
        network = self.settings.network
        if not network:
            return
        inspected = subprocess.run(
            ["docker", "network", "inspect", network], capture_output=True, text=True, timeout=10, check=False
        )
        if inspected.returncode == 0:
            return
        if "not found" not in inspected.stderr.lower() and "no such network" not in inspected.stderr.lower():
            raise RuntimeError("Cannot inspect the QLever gateway network: " + inspected.stderr.strip())
        created = subprocess.run(
            ["docker", "network", "create", "--driver", "bridge", "--label", "org.stack2graph.qlever=service", network],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if created.returncode != 0:
            raise RuntimeError("Cannot create the QLever gateway network: " + created.stderr.strip())

    def _preflight(self, candidate: Path) -> IndexManifest:
        """Verify runtime identity and the declared native index file inventory."""
        layout = ArtifactLayout(candidate)
        manifest = IndexManifest.from_json(json_object(parse_json(layout.path("index.json").read_text())))
        if manifest.image_digest != self.settings.image_digest or manifest.cli_version != self.version():
            raise ValueError("Index runtime identity differs from configured runtime")
        with verify_file(layout, manifest.files).open() as stream:
            for line in stream:
                reference = FileRef.from_json(json_object(parse_json(line)))
                path = layout.path(reference.path)
                if path.is_symlink() or not path.is_file() or path.stat().st_size != reference.bytes:
                    raise ValueError(f"QLever index file inventory differs: {reference.path}")
        return manifest

    def inspect(self, candidate: Path) -> IndexManifest:
        """Verify an index and cache its manifest for the next start."""
        manifest = self._preflight(candidate)
        self._inspected = (candidate, manifest)
        return manifest

    def start(self, candidate: Path) -> str:
        """Start a candidate after checking its recorded or cached provenance."""
        inspected, self._inspected = self._inspected, None
        if inspected is not None and inspected[0] == candidate:
            manifest = IndexManifest.from_json(json_object(parse_json((candidate / "index.json").read_text())))
            if manifest != inspected[1]:
                raise ValueError("Index provenance changed after deployment inspection")
        else:
            manifest = self._preflight(candidate)
        name = self._container_name(manifest)
        return self.serve(candidate, name)

    def serve(self, candidate: Path, name: str) -> str:
        """Serve an inspected graph index or an independently validated empty index."""
        self._validate_name(name)
        if not candidate.is_absolute() or candidate.resolve() != candidate or not candidate.is_dir():
            raise ValueError("QLever index directory must be absolute, existing and have no symlinks")
        endpoint = f"http://{self.settings.host}:{self.settings.port}"
        if self._container_running(name):
            if not self._matches_index(name, candidate):
                raise ValueError("Owned QLever service is serving a different index")
            self._check_binding(name)
            self._wait_ready(name, endpoint, allow_restart=True)
            return endpoint
        self.stop_named(name, candidate)
        try:
            self._start_container(candidate, name)
            self._check_binding(name)
            self._wait_ready(name, endpoint)
        except ValueError, RuntimeError, OSError:
            self.stop_named(name, candidate)
            raise
        return endpoint

    def _check_binding(self, name: str) -> None:
        """Require the owned container's HTTP port to bind only to loopback."""
        result = subprocess.run(["docker", "port", name], capture_output=True, text=True, timeout=10, check=False)
        expected = f"{self.settings.port}/tcp -> 127.0.0.1:{self.settings.port}"
        if result.returncode != 0 or result.stdout.strip() != expected:
            raise ValueError("Owned QLever HTTP binding must be loopback-only; run down then up to replace it")

    def _wait_ready(self, name: str, endpoint: str, *, allow_restart: bool = False) -> None:
        """Poll the owned container and its SPARQL endpoint until ready or timed out."""
        from graph.adapter.sparql import SparqlClient

        deadline = time.monotonic() + self.settings.timeout_seconds
        client = SparqlClient(endpoint, min(self.settings.timeout_seconds, 5))
        while True:
            running = self._container_running(name)
            if running is None or (not running and not allow_restart):
                raise RuntimeError("Owned QLever container exited before becoming ready")
            if running:
                try:
                    if client.ask("ASK {}"):
                        return
                except ValueError, RuntimeError, OSError:
                    if time.monotonic() >= deadline:
                        raise
            if time.monotonic() >= deadline:
                raise RuntimeError("Owned QLever container did not become ready")
            time.sleep(0.2)

    def _start_container(self, candidate: Path, name: str) -> None:
        """Keep HTTP local; an optional private network connects the shared TLS gateway."""
        settings = self.settings
        command = [
            "docker",
            "run",
            "--detach",
            "--init",
            "--restart",
            "no" if settings.instance_key else "unless-stopped",
            "--name",
            name,
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--mount",
            f"type=bind,source={candidate},target=/index",
            "--workdir",
            "/index",
            "--publish",
            f"127.0.0.1:{settings.port}:{settings.port}",
        ]
        # Candidate verification has its own instance and must never receive public traffic.
        if settings.network and not settings.instance_key:
            command.extend(("--network", settings.network, "--network-alias", "qlever"))
        command.extend(
            (
                "--entrypoint",
                "qlever-server",
                settings.image_digest,
                "-i",
                "kg",
                "-p",
                str(settings.port),
                "-m",
                f"{settings.query_memory_bytes}B",
                "-c",
                f"{settings.query_memory_bytes // 4}B",
                "-e",
                f"{settings.query_memory_bytes // 8}B",
                "-s",
                f"{settings.timeout_seconds}s",
            )
        )
        with (candidate / "start-command.log").open("wb") as output:
            result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=30, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"QLever start failed; see {candidate / 'start-command.log'}")

    def _container_name(self, manifest: IndexManifest) -> str:
        """Derive an owned server name from a valid index identity."""
        if re.fullmatch(r"[0-9a-f]{20}", manifest.index_id) is None:
            raise ValueError("Invalid owned QLever container identity")
        if self.settings.server_name and not self.settings.instance_key:
            return self.settings.server_name
        suffix = "-" + self.settings.instance_key if self.settings.instance_key else ""
        return "stack2graph-kg-server-" + manifest.index_id + suffix

    def _container_running(self, name: str) -> bool | None:
        """Read owned container state, or return None when it does not exist."""
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.Config.Image}} {{.State.Running}}", name],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if result.returncode != 0:
            if "no such object" in result.stderr.lower() or "no such container" in result.stderr.lower():
                return None
            raise RuntimeError("Cannot inspect the owned QLever container: " + result.stderr.strip())
        image, running = result.stdout.strip().split()
        if image != self.settings.image_digest or running not in ("true", "false"):
            raise ValueError("Owned container runtime identity differs from the configured image")
        return running == "true"

    def stop(self, candidate: Path) -> None:
        # Cleanup must work even when an index is damaged or the CLI is unavailable.
        """Stop a candidate's container after checking runtime ownership."""
        manifest = IndexManifest.from_json(json_object(parse_json((candidate / "index.json").read_text())))
        if manifest.image_digest != self.settings.image_digest:
            raise ValueError("Refusing to control another runtime image")
        name = self._container_name(manifest)
        self.stop_named(name, candidate)

    def _validate_name(self, name: str) -> None:
        """Reject container names outside the configured ownership pattern."""
        if self.settings.server_name and name == self.settings.server_name:
            return
        if re.fullmatch(r"stack2graph-kg-(server|empty|index)-[0-9a-f]{20}(-[a-z0-9-]{1,64})?", name) is None:
            raise ValueError("Invalid owned QLever container name")

    def stop_named(self, name: str, directory: Path) -> None:
        """Stop and remove only the owned container matching this index directory."""
        self._validate_name(name)
        if self._container_running(name) is None:
            return
        if not self._matches_index(name, directory):
            return
        with (directory / "stop-command.log").open("wb") as output:
            for command in (["docker", "stop", "--time", "30", name], ["docker", "rm", name]):
                result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=40, check=False)
                if result.returncode != 0:
                    raise RuntimeError("Failed to stop/remove the exact owned QLever container")

    def _matches_index(self, name: str, directory: Path) -> bool:
        """Check that a fixed-name service mounts the requested index directory."""
        if name != self.settings.server_name:
            return True
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                '{{range .Mounts}}{{if eq .Destination "/index"}}{{.Source}}{{end}}{{end}}',
                name,
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError("Cannot inspect owned QLever index mount: " + result.stderr.strip())
        return result.stdout.strip() == str(directory)
