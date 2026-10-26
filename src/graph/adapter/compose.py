"""Publish verified index selections through the Compose-managed service."""

import os
import re
import subprocess
from pathlib import Path

from graph.adapter.qlever import QLeverAdapter
from graph.domain.index import IndexManifest, QLeverSettings
from utils.artifacts import atomic_write_bytes, fsync_directory
from utils.config import REPOSITORY
from utils.types import json_array, json_object, json_string, parse_json


class ComposeQLeverRuntime:
    def __init__(self, settings: QLeverSettings, root: Path, index_root: Path) -> None:
        """Bind QLever settings, deployment paths, and the Compose service name."""
        self.settings, self.root, self.index_root = settings, root, index_root
        self.directory = REPOSITORY / "deployment" / "qlever"
        self.name = settings.server_name or settings.compose_project + "-qlever-1"
        self.selection = root / "active-index"
        self.verifier = QLeverAdapter(settings)

    def inspect(self, candidate: Path) -> IndexManifest:
        """Validate the candidate path and inspect its native index manifest."""
        self._index_name(candidate)
        return self.verifier.inspect(candidate)

    def _index_name(self, candidate: Path) -> str:
        """Require a direct, safely named child of the configured index root."""
        if (
            not candidate.is_absolute()
            or candidate.resolve() != candidate
            or candidate.parent != self.index_root
            or re.fullmatch(r"[a-zA-Z0-9_-]+", candidate.name) is None
        ):
            raise ValueError("Published index must be a direct directory inside the configured index root")
        return candidate.name

    def running(self) -> bool:
        """Read container state after verifying Compose ownership and mounts."""
        result = subprocess.run(
            ["docker", "container", "inspect", self.name],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if result.returncode:
            if "no such object" in result.stderr.lower() or "no such container" in result.stderr.lower():
                return False
            raise RuntimeError("Cannot inspect the Compose QLever container: " + result.stderr.strip())
        container = json_object(json_array(parse_json(result.stdout))[0])
        config = json_object(container["Config"])
        labels = json_object(config["Labels"])
        mounts = {
            json_string(mount["Destination"]): json_string(mount["Source"])
            for value in json_array(container["Mounts"])
            for mount in (json_object(value),)
        }
        if (
            labels.get("com.docker.compose.project") != self.settings.compose_project
            or labels.get("com.docker.compose.service") != "qlever"
            or config["Image"] != self.settings.image_digest
            or mounts.get("/indexes") != str(self.index_root)
            or mounts.get("/deployment") != str(self.root)
        ):
            raise ValueError("Refusing to control a QLever container outside this Compose deployment")
        running = json_object(container["State"])["Running"]
        if not isinstance(running, bool):
            raise ValueError("Invalid Compose QLever running state")
        return running

    def _compose(self, *arguments: str) -> None:
        """Run a QLever Compose command with service-specific environment values."""
        settings = self.settings
        environment = {
            **os.environ,
            "STACK2GRAPH_DATA_ROOT": str(self.index_root.parent),
            "STACK2GRAPH_QLEVER_UID": str(os.getuid()),
            "STACK2GRAPH_QLEVER_GID": str(os.getgid()),
            "STACK2GRAPH_QLEVER_COMPOSE_PROJECT": settings.compose_project,
            "STACK2GRAPH_QLEVER_CONTAINER_NAME": self.name,
            "STACK2GRAPH_QLEVER_IMAGE": settings.image_digest,
            "STACK2GRAPH_QLEVER_INDEX_ROOT": str(self.index_root),
            "STACK2GRAPH_QLEVER_DEPLOYMENT_ROOT": str(self.root),
            "STACK2GRAPH_QLEVER_PORT": str(settings.port),
            "STACK2GRAPH_QLEVER_QUERY_RAM_BYTES": str(settings.query_memory_bytes),
            "STACK2GRAPH_QLEVER_TIMEOUT_SECONDS": str(settings.timeout_seconds),
            "STACK2GRAPH_QLEVER_NETWORK": settings.network or settings.compose_project,
        }
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--env-file",
                "/dev/null",
                "--project-name",
                settings.compose_project,
                "--project-directory",
                str(self.directory),
                "--file",
                str(self.directory / "compose.yml"),
                *arguments,
            ],
            env=environment,
            capture_output=True,
            text=True,
            timeout=settings.timeout_seconds + 120,
            check=False,
        )
        if result.returncode:
            logs = subprocess.run(
                ["docker", "logs", "--tail", "200", self.name],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            raise RuntimeError(
                "QLever Compose failed: " + result.stderr.strip() + "\n" + logs.stdout[:6000] + logs.stderr[:6000]
            )

    def start_service(self) -> str:
        """Start the owned Compose service and return its HTTP endpoint."""
        self.running()  # Validate ownership before Compose can recreate an existing container.
        self._compose("up", "-d", "--wait", "--wait-timeout", str(self.settings.timeout_seconds), "qlever")
        return f"http://{self.settings.host}:{self.settings.port}"

    def start(self, candidate: Path) -> str:
        """Select an index, recreate the Compose service, and wait for readiness."""
        name = self._index_name(candidate)
        self.running()
        atomic_write_bytes(self.selection, (name + "\n").encode())
        self._compose(
            "up", "-d", "--force-recreate", "--wait", "--wait-timeout", str(self.settings.timeout_seconds), "qlever"
        )
        return f"http://{self.settings.host}:{self.settings.port}"

    def start_empty(self) -> str:
        """Clear the active-index selection and start the empty-index service."""
        self.running()
        self.selection.unlink(missing_ok=True)
        fsync_directory(self.root)
        return self.start_service()

    def stop(self, candidate: Path) -> None:
        """Stop the service only if the supplied candidate is selected."""
        if self.selection.exists() and self.selection.read_text().strip() == self._index_name(candidate):
            self.stop_service()

    def stop_service(self) -> None:
        """Stop the owned Compose service if it is running."""
        if self.running():
            self._compose("stop", "qlever")
