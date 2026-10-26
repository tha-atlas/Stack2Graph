"""Validate connection settings without opening files or database connections."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PostgreSQLConfig:
    host: str
    port: int
    database: str
    user: str = field(repr=False)
    password: str = field(repr=False)
    pool_size: int = 1
    connect_timeout: int = 10
    retry_attempts: int = 3

    def __post_init__(self) -> None:
        """Validate required connection settings, ports, pooling, and retries."""
        if not self.host or not self.database or not self.user or not self.password:
            raise ValueError("PostgreSQL connection settings are incomplete")
        if not 0 < self.port < 65536 or not 1 <= self.pool_size <= 64:
            raise ValueError("Invalid PostgreSQL port or pool size (1..64)")
        if self.connect_timeout <= 0 or not 1 <= self.retry_attempts <= 10:
            raise ValueError("Invalid PostgreSQL timeout or retry attempts")
