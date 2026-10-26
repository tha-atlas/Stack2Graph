"""Switch graph indexes durably while retaining a verified previous index."""

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from graph.adapter.qlever import QLeverAdapter
from graph.domain.index import IndexManifest, QLeverSettings, QueryAcceptance
from utils.artifacts import atomic_write_json, fsync_directory
from utils.manifest import SnapshotLock
from utils.types import JSONObject, json_array, json_object, json_string, parse_json


@dataclass(frozen=True)
class DeploymentState:
    active_index: str | None
    previous_index: str | None
    endpoint: str | None

    def to_json(self) -> JSONObject:
        """Serialize active and previous index paths and the service endpoint."""
        return {"active_index": self.active_index, "previous_index": self.previous_index, "endpoint": self.endpoint}

    @classmethod
    def from_json(cls, value: JSONObject) -> DeploymentState:
        """Decode optional deployment paths and the service endpoint."""

        def optional(key: str) -> str | None:
            """Read a nullable deployment string field."""
            return None if value[key] is None else json_string(value[key])

        return cls(optional("active_index"), optional("previous_index"), optional("endpoint"))


class DeploymentRuntime(Protocol):
    def inspect(self, candidate: Path) -> IndexManifest:
        """Verify a candidate and return its index manifest."""
        ...

    def start(self, candidate: Path) -> str:
        """Start the candidate index and return its service endpoint."""
        ...

    def stop(self, candidate: Path) -> None:
        """Stop the service associated with the supplied candidate."""
        ...


def read_index(candidate: Path) -> IndexManifest:
    """Decode a candidate's recorded index manifest."""
    return IndexManifest.from_json(json_object(parse_json((candidate / "index.json").read_text())))


def read_acceptance(candidate: Path) -> QueryAcceptance:
    """Read recorded query acceptance, requiring a boolean pass result."""
    value = json_object(parse_json((candidate / "acceptance.json").read_text()))
    passed = value["passed"]
    if not isinstance(passed, bool):
        raise ValueError("Query acceptance must be boolean")
    return QueryAcceptance(
        json_string(value["index_digest"]),
        passed,
        tuple(json_string(name) for name in json_array(value["checked_queries"])),
    )


class DeploymentController:
    def __init__(
        self, settings: QLeverSettings, deployment_root: Path, *, adapter: DeploymentRuntime | None = None
    ) -> None:
        """Create a deployment root and bind its settings and runtime adapter."""
        if not deployment_root.is_absolute() or deployment_root.resolve() != deployment_root:
            raise ValueError("Deployment root must be absolute and have no symlinks")
        self.settings, self.root = settings, deployment_root
        self.adapter: DeploymentRuntime = QLeverAdapter(settings) if adapter is None else adapter
        self.root.mkdir(parents=True, exist_ok=True)

    def _state(self) -> DeploymentState:
        """Read recorded deployment state, or return an empty initial state."""
        path = self.root / "state.json"
        return (
            DeploymentState(None, None, None)
            if not path.exists()
            else DeploymentState.from_json(json_object(parse_json(path.read_text())))
        )

    def _write(self, state: DeploymentState) -> None:
        """Atomically replace the deployment state file."""
        atomic_write_json(self.root / "state.json", state.to_json())

    def _clear_transition(self) -> None:
        """Remove the transition record and flush deployment directory metadata."""
        (self.root / "transition.json").unlink(missing_ok=True)
        fsync_directory(self.root)

    def _recover_interrupted(self) -> None:
        """Stop an interrupted candidate and restore the recorded previous state."""
        path = self.root / "transition.json"
        if not path.exists():
            return
        value = json_object(parse_json(path.read_text()))
        previous = DeploymentState.from_json(json_object(value["before"]))
        candidate = Path(json_string(value["candidate"]))
        self.adapter.stop(candidate)
        if previous.active_index is not None:
            self.adapter.start(Path(previous.active_index))
        self._write(previous)
        self._clear_transition()

    def _switch(self, candidate: Path, acceptance: QueryAcceptance) -> DeploymentState:
        """Activate an accepted index and restore prior state if switching fails."""
        if not candidate.is_absolute() or candidate.resolve() != candidate:
            raise ValueError("Candidate path must be absolute and have no symlinks")
        index = self.adapter.inspect(candidate)
        required = {"Posts", "Comments", "Tags", "Votes", "PostLinks"}
        if (
            not index.verified
            or index.image_digest != self.settings.image_digest
            or not acceptance.passed
            or acceptance.index_digest != index.digest
            or not required.issubset(acceptance.checked_queries)
            or read_acceptance(candidate) != acceptance
        ):
            raise ValueError("Activation requires matching recorded successful query acceptance")
        before = self._state()
        if before.active_index == str(candidate):
            endpoint = self.adapter.start(candidate)
            restored = DeploymentState(before.active_index, before.previous_index, endpoint)
            self._write(restored)
            return restored
        atomic_write_json(self.root / "transition.json", {"before": before.to_json(), "candidate": str(candidate)})
        try:
            if before.active_index is not None:
                self.adapter.stop(Path(before.active_index))
            endpoint = self.adapter.start(candidate)
            state = DeploymentState(str(candidate), before.active_index or before.previous_index, endpoint)
            self._write(state)
            self._clear_transition()
            return state
        except BaseException:
            try:
                self.adapter.stop(candidate)
            except RuntimeError:
                pass  # A start failure may occur before the owned process exists.
            if before.active_index is not None:
                try:
                    self.adapter.start(Path(before.active_index))
                except BaseException:
                    self._write(DeploymentState(None, before.active_index, None))
                    raise
            self._write(before)
            self._clear_transition()
            raise

    def activate(self, candidate: Path, acceptance: QueryAcceptance) -> DeploymentState:
        """Lock deployment, recover interrupted work, and switch to the candidate."""
        with SnapshotLock(self.root):
            self._recover_interrupted()
            return self._switch(candidate, acceptance)

    def start(self, candidate: Path) -> str:
        """Activate a candidate using its recorded successful query acceptance."""
        state = self.activate(candidate, read_acceptance(candidate))
        if state.endpoint is None:
            raise RuntimeError("Verified service did not produce an endpoint")
        return state.endpoint

    def status(self) -> DeploymentState:
        """Read deployment state, rejecting an unresolved interrupted transition."""
        if (self.root / "transition.json").exists():
            raise RuntimeError("Interrupted deployment switch requires start/rollback recovery")
        return self._state()

    def recover(self) -> DeploymentState:
        """Restore an interrupted switch before managed service startup."""
        with SnapshotLock(self.root):
            self._recover_interrupted()
            return self._state()

    def stop(self) -> None:
        """Stop the active index under a lock and retain it for rollback."""
        with SnapshotLock(self.root):
            self._recover_interrupted()
            state = self._state()
            if state.active_index is not None:
                self.adapter.stop(Path(state.active_index))
            self._write(DeploymentState(None, state.active_index or state.previous_index, None))

    def rollback(self) -> DeploymentState:
        """Activate the previous verified index under the deployment lock."""
        with SnapshotLock(self.root):
            self._recover_interrupted()
            state = self._state()
            if state.previous_index is None:
                raise ValueError("No previous verified index available")
            candidate = Path(state.previous_index)
            return self._switch(candidate, read_acceptance(candidate))
