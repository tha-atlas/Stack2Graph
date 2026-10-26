"""Load validation read limits and shared environment settings."""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from selection.config import registry_path
from sql.config import load_postgres_config
from sql.domain.postgres import PostgreSQLConfig
from utils.config import DataPaths, load_data_paths, load_environment, run_root, validate_item_id
from vector.config import load_qdrant_config
from vector.domain.config import QdrantConfig


@dataclass(frozen=True)
class ValidationLimits:
    read_rows: int = 1024
    read_bytes: int = 16 * 1024**2

    def __post_init__(self) -> None:
        """Require positive row and byte limits for validation reads."""
        if min(self.read_rows, self.read_bytes) <= 0:
            raise ValueError("Validation limits must be positive")


@dataclass(frozen=True)
class ValidationConfig:
    paths: DataPaths
    postgres: PostgreSQLConfig
    snapshot: str
    qdrant: QdrantConfig
    registry_path: Path
    graph_root: Path
    vector_root: Path
    run_root: Path
    limits: ValidationLimits


def load_validation_config(environ: Mapping[str, str], env_file: Path | None = None) -> ValidationConfig:
    """Load validation settings and resolve external artifact directories."""
    values = load_environment(environ, env_file)
    paths = load_data_paths({}, values)
    snapshot = values.get("STACK2GRAPH_VECTOR_SNAPSHOT", "")
    validate_item_id(snapshot)

    def number(name: str, default: int) -> int:
        """Read an integer validation setting or use its default."""
        return int(values.get("STACK2GRAPH_VALIDATION_" + name, str(default)))

    return ValidationConfig(
        paths,
        load_postgres_config(values),
        snapshot,
        load_qdrant_config(values),
        registry_path(values),
        run_root(values, paths.data_root, "GRAPH"),
        run_root(values, paths.data_root, "VECTOR"),
        run_root(values, paths.data_root, "VALIDATION"),
        ValidationLimits(
            number("READ_ROWS", 1024),
            number("READ_BYTES", 16 * 1024**2),
        ),
    )
