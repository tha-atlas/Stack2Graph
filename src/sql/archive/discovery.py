"""Atomically cache publisher release identities during discovery."""

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sql.domain.models import SnapshotIdentity
from utils.manifest import ManifestStore
from utils.types import JSONObject, JSONValue, json_object, json_string


@dataclass(frozen=True)
class DiscoveryEntry:
    reference: str
    checked_at: datetime
    identity: SnapshotIdentity | None
    rejection: str | None = None

    def to_json(self) -> JSONObject:
        """Serialize a discovery reference, check time, and identity or rejection."""
        return {
            "reference": self.reference,
            "checked_at": self.checked_at.isoformat(),
            "identity": self.identity.to_json() if self.identity is not None else None,
            "rejection": self.rejection,
        }

    @classmethod
    def from_json(cls, value: JSONObject) -> DiscoveryEntry:
        """Decode cached discovery evidence, requiring an aware timestamp."""
        checked_at = datetime.fromisoformat(json_string(value["checked_at"]))
        if checked_at.tzinfo is None:
            raise ValueError("Discovery timestamp must include a timezone")
        identity = value["identity"]
        rejection = value.get("rejection")
        return cls(
            json_string(value["reference"]),
            checked_at.astimezone(UTC),
            SnapshotIdentity.from_dict(json_object(identity)) if identity is not None else None,
            json_string(rejection) if rejection is not None else None,
        )


class DiscoveryCache:
    def __init__(self, directory: Path) -> None:
        """Locate the discovery catalog manifest in the supplied directory."""
        self.store = ManifestStore(directory / "catalogue.json")

    def read(self) -> dict[str, DiscoveryEntry]:
        """Read version 1 discovery entries and verify their item identities."""
        state = self.store.read()
        if not state:
            return {}
        if state.get("version") != 1:
            raise ValueError("Unsupported discovery cache version")
        entries = {
            item_id: DiscoveryEntry.from_json(json_object(entry))
            for item_id, entry in json_object(state["entries"]).items()
        }
        if any(entry.identity is not None and entry.identity.item_id != item_id for item_id, entry in entries.items()):
            raise ValueError("Discovery entry identity mismatch")
        return entries

    def write(self, entries: dict[str, DiscoveryEntry], checked_at: datetime) -> None:
        """Atomically store discovery entries and the catalog check time."""
        payload: dict[str, JSONValue] = {item_id: entry.to_json() for item_id, entry in entries.items()}
        self.store.write({"version": 1, "catalogue_checked_at": checked_at.isoformat(), "entries": payload})
