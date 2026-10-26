"""Start a persistent empty service and publish verified graph builds to it."""

from pathlib import Path

from graph.adapter.compose import ComposeQLeverRuntime
from graph.adapter.sparql import SparqlClient
from graph.deployment.controller import DeploymentController, read_acceptance
from graph.domain.index import QLeverSettings
from utils.manifest import SnapshotLock
from utils.types import JSONObject


class ManagedGraphService:
    def __init__(self, settings: QLeverSettings, root: Path, index_root: Path) -> None:
        """Bind the Compose runtime and controller to the deployment directories."""
        self.settings, self.root = settings, root
        self.runtime = ComposeQLeverRuntime(settings, root, index_root)
        self.controller = DeploymentController(settings, root, adapter=self.runtime)
        self.endpoint = f"http://{settings.host}:{settings.port}"

    def _require_running(self) -> None:
        """Require an already running managed QLever container."""
        if not self.runtime.running():
            raise ValueError("Start QLever first: ./deployment/qlever/run.sh up -d --wait")

    def require_started(self) -> None:
        """Require a running service that passes a SPARQL readiness probe."""
        self._require_running()
        try:
            ready = SparqlClient(self.endpoint, min(self.settings.timeout_seconds, 5)).ask("ASK {}")
        except (ValueError, RuntimeError, OSError) as error:
            raise RuntimeError("QLever is unavailable; run ./deployment/qlever/run.sh up -d --wait") from error
        if not ready:
            raise RuntimeError("QLever did not pass its readiness probe")

    def start(self) -> str:
        """Restore the recorded index or start an empty service under a lock."""
        with SnapshotLock(self.root / "service-control"):
            self._restore()
            return self.endpoint

    def _restore(self) -> None:
        """Recover deployment state and start its recorded or empty index."""
        state = self.controller.recover()
        selected = state.active_index or state.previous_index
        if selected is None:
            self.runtime.start_empty()
        else:
            self.controller.start(Path(selected))

    def prepare_build(self) -> None:
        """Require a running service and restore interrupted or unhealthy state."""
        with SnapshotLock(self.root / "service-control"):
            self._require_running()
            if (self.root / "transition.json").exists():
                self._restore()
            try:
                self.require_started()
            except RuntimeError:
                # Restore the recorded selection if a running container cannot answer queries.
                self._restore()
                self.require_started()

    def publish(self, index: Path) -> str:
        """
        Activate a verified index under the service lock and return its
        endpoint.
        """
        with SnapshotLock(self.root / "service-control"):
            if self.runtime.running() and (self.root / "transition.json").exists():
                self._restore()
            self.require_started()
            acceptance = read_acceptance(index)
            before = self.controller.recover()
            try:
                self.controller.activate(index, acceptance)
            except BaseException:
                if before.active_index is None:
                    self.runtime.start_empty()
                raise
            return self.endpoint

    def stop(self) -> None:
        """Stop the managed Compose service under its control lock."""
        with SnapshotLock(self.root / "service-control"):
            self.runtime.stop_service()

    def rollback(self) -> None:
        """Require service readiness and activate the previous verified index."""
        with SnapshotLock(self.root / "service-control"):
            self.require_started()
            self.controller.rollback()

    def status(self) -> JSONObject:
        """Report service running state, health, loaded data, and endpoint."""
        enabled = self.runtime.running()
        state = self.controller.status()
        healthy = False
        if enabled:
            try:
                self.require_started()
                healthy = True
            except ValueError, RuntimeError, OSError:
                pass
        return {
            "enabled": enabled,
            "healthy": healthy,
            "data_loaded": state.active_index is not None,
            "endpoint": self.endpoint if enabled else None,
        }
