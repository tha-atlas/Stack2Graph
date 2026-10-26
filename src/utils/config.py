"""Read literal settings and validate external snapshot storage paths."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[2]
KEYS = {
    "data_root": "STACK2GRAPH_DATA_ROOT",
    "data_dir": "STACK2GRAPH_DATA_DIR",
    "xml_path": "STACK2GRAPH_DATA_XML_PATH",
    "postgres_path": "STACK2GRAPH_DATA_POSTGRES_PATH",
}


def read_env(path: Path) -> dict[str, str]:
    """Read literal environment assignments, rejecting shell expansion."""
    values: dict[str, str] = {}
    for number, line in enumerate(path.read_text().splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"(?:export\s+)?([A-Za-z_][A-Za-z_0-9]*)\s*=\s*(.*)", line)
        if not match:
            raise ValueError(f"{path}:{number}: expected a literal assignment")
        key, value = match.groups(default="")
        if value[:1] in ('"', "'"):
            if len(value) < 2 or value[-1] != value[0]:
                raise ValueError(f"{path}:{number}: unmatched quote")
            value = value[1:-1]
        else:
            value = value.split(" #", 1)[0].rstrip()
        if "$" in value or "`" in value or "\x00" in value:
            raise ValueError(f"{path}:{number}: shell expansion is unsupported")
        values[key] = value
    return values


def load_environment(environ: Mapping[str, str], env_file: Path | None = None) -> dict[str, str]:
    """
    Load the root .env by default, then apply environment overrides.

    Args:
        environ: Values that override assignments read from the file.
        env_file: Alternative file; None selects the repository-root .env.

    Returns:
        Merged values. An absent default .env contributes no assignments.

    """
    path = REPOSITORY / ".env" if env_file is None else env_file
    values = read_env(path) if env_file is not None or path.exists() else {}
    values.update(environ)
    return values


def _absolute(value: str) -> Path:
    """Resolve an absolute path or raise ValueError for a relative path."""
    path = Path("/" + value.lstrip("/")) if value.startswith("/") else Path(value)
    if not path.is_absolute():
        raise ValueError(f"Expected an absolute data path: {value}")
    return path.resolve()


def outside_repository(value: str | Path) -> Path:
    """Require an absolute external data directory, including resolved symlinks."""
    path = Path(value)
    if not path.is_absolute():
        raise ValueError("Data paths must be absolute")
    path = path.resolve()
    if path == REPOSITORY or path.is_relative_to(REPOSITORY) or REPOSITORY.is_relative_to(path):
        raise ValueError(f"Data must stay outside the repository: {path}")
    return path


_RUN_DIR_DEFAULTS: dict[str, str] = {
    "VALIDATION": "validation-reports",
    "GRAPH": "graph-runs",
    "VECTOR": "vector-runs",
}


def default_run_root(data_root: Path, area: str) -> Path:
    """Return an area's default run directory under the data root."""
    name = _RUN_DIR_DEFAULTS.get(area)
    if name is None:
        raise ValueError(f"Unknown run-directory area: {area!r}")
    return data_root / name


def run_root(values: Mapping[str, str], data_root: Path, area: str) -> Path:
    """Resolve the area's run-directory setting outside the repository."""
    return outside_repository(values.get(f"STACK2GRAPH_{area}_RUN_DIR", str(default_run_root(data_root, area))))


@dataclass(frozen=True)
class DataPaths:
    data_root: Path
    data_dir: Path
    xml_root: Path
    postgres_root: Path

    @property
    def postgres_cluster(self) -> Path:
        """The PostgreSQL cluster directory under the configured root."""
        return self.postgres_root / "cluster"

    def __post_init__(self) -> None:
        """Validate external data paths and separate XML and database roots."""
        for path in (self.data_root, self.data_dir, self.xml_root, self.postgres_root):
            if not path.is_absolute() or path != path.resolve():
                raise ValueError(f"Path must be absolute and resolved: {path}")
            outside_repository(path)
            if path != self.postgres_root and not path.is_relative_to(self.data_root):
                raise ValueError(f"Data path escapes configured root: {path}")
        if (
            self.xml_root == self.postgres_root
            or self.xml_root.is_relative_to(self.postgres_root)
            or self.postgres_root.is_relative_to(self.xml_root)
        ):
            raise ValueError("XML and PostgreSQL roots must be separate")


def load_data_paths(
    overrides: Mapping[str, str],
    environ: Mapping[str, str],
    env_file: Path | None = None,
) -> DataPaths:
    """
    Resolve storage paths from file, environment, and explicit overrides.

    Args:
        overrides: Explicit values keyed by the supported storage option names.
        environ: Environment values applied after the optional file.
        env_file: File to read before environment and explicit overrides.

    Returns:
        Resolved data, archive/XML, and PostgreSQL paths.

    Raises:
        ValueError: A key or storage path violates the storage contract.

    """
    values = read_env(env_file) if env_file else {}
    values.update(environ)
    if any(key.startswith("STACK2GRAPH_DATA_") and key not in KEYS.values() for key in values):
        raise ValueError("Unsupported storage configuration; use the documented PostgreSQL path keys")
    if any(key not in KEYS for key in overrides):
        raise ValueError("Unsupported storage configuration override")
    values.update({KEYS[k]: v for k, v in overrides.items() if k in KEYS and v is not None})
    root_value = values.get(KEYS["data_root"])
    if root_value is None:
        xdg = Path(values.get("XDG_DATA_HOME", ""))
        root_value = str((xdg if xdg.is_absolute() else Path.home() / ".local" / "share") / "stack2graph")
    root = _absolute(root_value)
    data = _absolute(values.get(KEYS["data_dir"], str(root / "datadump")))
    return DataPaths(
        root,
        data,
        _absolute(values.get(KEYS["xml_path"], str(data / "xml"))),
        _absolute(values.get(KEYS["postgres_path"], str(data / "postgres"))),
    )


def validate_item_id(item_id: str) -> None:
    """Reject snapshot IDs that contain unsafe path characters."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}", item_id) or ".." in item_id:
        raise ValueError(f"Unsafe snapshot item ID: {item_id!r}")


@dataclass(frozen=True)
class SnapshotPaths:
    item_id: str
    archive_dir: Path
    xml: Path
    database_dir: Path
    temporary: Path
    state: Path

    @classmethod
    def from_config(cls, paths: DataPaths, item_id: str) -> SnapshotPaths:
        """Build snapshot paths and reject symlinks or paths outside their roots."""
        validate_item_id(item_id)
        result = cls(
            item_id,
            paths.data_dir / "archive" / item_id,
            paths.xml_root / item_id,
            paths.postgres_root / "snapshots" / item_id,
            paths.data_dir / "temporary" / item_id,
            paths.data_dir / "snapshots" / item_id,
        )
        for path in (result.archive_dir, result.xml, result.database_dir, result.temporary, result.state):
            root = paths.postgres_root if path == result.database_dir else paths.data_root
            if path.resolve() != path or not path.resolve().is_relative_to(root):
                raise ValueError(f"Snapshot path follows a symlink or escapes root: {path}")
        return result

    @property
    def archive(self) -> Path:
        """The Stack Overflow archive path for this snapshot."""
        return self.archive_dir / "stackoverflow.com.7z"

    @property
    def publication(self) -> Path:
        """The database publication metadata path."""
        return self.database_dir / "publication.json"

    @property
    def workspace(self) -> str:
        """The parent directory of this snapshot's state directory."""
        return str(self.state.parent)

    @property
    def manifest(self) -> Path:
        """The snapshot manifest path."""
        return self.state / "manifest.json"
