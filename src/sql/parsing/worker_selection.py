"""Configure and cache XML worker counts per machine and table."""

import platform
from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path

from sql.domain.schema import TABLES
from utils.config import read_env
from utils.manifest import ManifestStore
from utils.types import json_integer


def load_xml_workers(environ: Mapping[str, str], env_file: Path | None = None) -> int | None:
    """Parse a fixed worker count or select automatic calibration."""
    values = read_env(env_file) if env_file is not None else {}
    values.update(environ)
    value = values.get("STACK2GRAPH_XML_WORKERS", "auto").strip().lower()
    if value in ("", "auto"):
        return None
    try:
        workers = int(value)
    except ValueError:
        raise ValueError("STACK2GRAPH_XML_WORKERS must be auto or a positive integer") from None
    if workers <= 0:
        raise ValueError("STACK2GRAPH_XML_WORKERS must be auto or a positive integer")
    return workers


class XMLWorkerProfile:
    def __init__(self, directory: Path, table: str, *, machine: str | None = None) -> None:
        """Choose a table- and machine-specific profile path without symlinks."""
        if table not in TABLES:
            raise ValueError("Unsupported XML worker profile table")
        if not directory.is_absolute() or directory.resolve() != directory:
            raise ValueError("XML worker profiles require an absolute directory without symlinks")
        identity = machine if machine is not None else f"{platform.node()}|{platform.machine()}"
        key = sha256(identity.encode("utf-8")).hexdigest()[:24]
        self.path = directory / f"{table}-{key}.json"

    def load(self) -> int | None:
        """Read an atomic profile; corrupt or unsupported profiles fail explicitly."""
        state = ManifestStore(self.path).read()
        if not state:
            if self.path.exists():
                raise ValueError("Invalid XML worker profile")
            return None
        if set(state) != {"version", "workers"} or json_integer(state["version"]) != 1:
            raise ValueError("Unsupported XML worker profile")
        workers = json_integer(state["workers"])
        if workers <= 0:
            raise ValueError("Invalid XML worker profile width")
        return workers

    def save(self, workers: int) -> None:
        """Atomically replace one small profile without changing environment files."""
        if workers <= 0:
            raise ValueError("Invalid XML worker profile width")
        ManifestStore(self.path).write({"version": 1, "workers": workers})
