"""Load PostgreSQL connection settings without executing SQL or connecting."""

from collections.abc import Mapping
from pathlib import Path

from sql.domain.postgres import PostgreSQLConfig
from utils.config import read_env


def load_postgres_config(environ: Mapping[str, str], env_file: Path | None = None) -> PostgreSQLConfig:
    """Read connection settings and the password from an absolute file path."""
    values = read_env(env_file) if env_file is not None else {}
    values.update(environ)
    prefix = "STACK2GRAPH_POSTGRES_"
    required = ("HOST", "PORT", "DATABASE", "USER", "PASSWORD_FILE")
    missing = [prefix + name for name in required if not values.get(prefix + name)]
    if missing:
        raise ValueError("Missing PostgreSQL configuration: " + ", ".join(missing))
    password_file = Path(values[prefix + "PASSWORD_FILE"])
    if not password_file.is_absolute() or password_file.is_symlink():
        raise ValueError("PostgreSQL password file must be absolute and regular")
    return PostgreSQLConfig(
        values[prefix + "HOST"],
        int(values[prefix + "PORT"]),
        values[prefix + "DATABASE"],
        values[prefix + "USER"],
        password_file.read_text().rstrip("\r\n"),
        int(values.get(prefix + "POOL_SIZE", "1")),
    )
