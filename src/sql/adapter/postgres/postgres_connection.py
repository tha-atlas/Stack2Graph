"""Open typed PostgreSQL connections and decode scalar dump values safely."""

from collections.abc import Sequence

from psycopg import Connection, Cursor, OperationalError
from psycopg.conninfo import make_conninfo
from psycopg.rows import RowMaker

from sql.domain.models import Row, RowValue
from sql.domain.postgres import PostgreSQLConfig
from sql.domain.rows import integer, text
from utils.types import json_object, json_string, parse_json


def row_factory(cursor: Cursor[Row]) -> RowMaker[Row]:
    """Build a row decoder from cursor column names."""
    names = tuple(column.name for column in cursor.description or ())

    def make_row(values: Sequence[RowValue]) -> Row:
        """Map values to columns and decode textual ExtraAttributes JSON."""
        row = dict(zip(names, values, strict=True))
        extra = row.get("ExtraAttributes")
        if isinstance(extra, str):
            row["ExtraAttributes"] = {key: json_string(value) for key, value in json_object(parse_json(extra)).items()}
        return row

    return make_row


def connection_info(config: PostgreSQLConfig) -> str:
    """Build connection parameters with the Stack2Graph application name."""
    return make_conninfo(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
        connect_timeout=config.connect_timeout,
        application_name="stack2graph",
    )


def _connection_failure_reason(error: OperationalError) -> str:
    """Return only fixed diagnostic text; raw driver errors can contain credentials."""
    message = str(error).lower()
    if "permission denied" in message:
        return "filesystem permission denied; align container UID/GID with cluster ownership"
    if "password authentication failed" in message or error.sqlstate == "28P01":
        return "authentication failed; check the local credential file and initialized role"
    if ("database" in message and "does not exist" in message) or error.sqlstate == "3D000":
        return "configured database does not exist; check initialized database settings"
    if "connection refused" in message:
        return "connection refused; check the configured host port and server"
    if "timeout" in message:
        return "connection timed out; check the host port, network and server"
    return "check local configuration and server health"


def connect(config: PostgreSQLConfig) -> Connection[Row]:
    """Open an autocommit connection with sanitized connection failures."""
    failure_type: type[OperationalError] = OperationalError
    try:
        return Connection[Row].connect(connection_info(config), row_factory=row_factory, autocommit=True)
    except failure_type as error:
        raise RuntimeError(f"PostgreSQL connection failed; {_connection_failure_reason(error)}") from None


def runtime_identity(connection: Connection[Row]) -> tuple[str, ...]:
    """Require PostgreSQL 18 and return server and driver versions."""
    import psycopg

    row = connection.execute(
        "SELECT current_setting('server_version') AS version, current_setting('server_version_num')::bigint AS number"
    ).fetchone()
    if integer(row, "number") // 10000 != 18:
        raise ValueError("This deployment requires stable PostgreSQL 18")
    return (text(row, "version"), psycopg.__version__)
