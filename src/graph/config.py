"""Load graph settings from the repository environment automatically."""

from collections.abc import Mapping
from pathlib import Path
from typing import Literal

from graph.domain.config import ExportConfig
from graph.domain.index import IndexConfig, QLeverSettings
from selection.config import registry_path
from sql.config import load_postgres_config
from utils.config import REPOSITORY, load_data_paths, load_environment, read_env, run_root, validate_item_id
from utils.resources import ResourceCaps, ResourcePlan, cap_budget, detect_limits, plan_resources, resource_budget


def deployment_root(config: IndexConfig, values: Mapping[str, str]) -> Path:
    """Resolve the QLever deployment directory from configuration."""
    return Path(
        values.get(
            "STACK2GRAPH_QLEVER_DEPLOYMENT_ROOT", str(load_data_paths({}, values).data_root / "qlever-deployment")
        )
    ).resolve()


def resource_caps(values: Mapping[str, str]) -> ResourceCaps:
    """Read graph CPU, memory, batching, spill, and disk reserve caps."""

    def setting(key: str, default: str = "") -> str:
        """Read a graph-prefixed setting or its default."""
        return values.get("STACK2GRAPH_GRAPH_" + key, default)

    def optional(key: str) -> int | None:
        """Read an optional integer graph setting, returning None if empty."""
        value = setting(key)
        return int(value) if value else None

    return ResourceCaps(
        threads=int(setting("WORKERS", "64")),
        memory_bytes=optional("RAM_BYTES"),
        batch_rows=int(setting("BATCH_ROWS", "4096")),
        batch_bytes=optional("BATCH_BYTES"),
        record_bytes=optional("RECORD_BYTES"),
        spill_bytes=int(setting("SPILL_BYTES", str(8 * 1024**3))),
        disk_reserve=int(setting("DISK_RESERVE_BYTES", str(256 * 1024**2))),
    )


def load_resource_plan(environ: Mapping[str, str], env_file: Path | None = None) -> ResourcePlan:
    """Load graph resource caps and build a scalable plan for this host."""
    return plan_resources(detect_limits(), resource_caps(load_environment(environ, env_file)), scalable=True)


def load_export_config(environ: Mapping[str, str], env_file: Path | None = None) -> ExportConfig:
    """Load source selection, artifact paths, and graph compression settings."""
    values = load_environment(environ, env_file)
    paths = load_data_paths({}, values)

    def setting(key: str, default: str = "") -> str:
        """Read a graph-prefixed setting or its default."""
        return values.get("STACK2GRAPH_GRAPH_" + key, default)

    snapshot = setting("SNAPSHOT")
    validate_item_id(snapshot)
    runs = run_root(values, paths.data_root, "GRAPH")
    compression = setting("COMPRESSION", "none")
    if compression not in ("none", "gzip"):
        raise ValueError("Graph compression must be none or gzip")
    kind: Literal["none", "gzip"] = "none" if compression == "none" else "gzip"
    raw_level = setting("COMPRESSION_LEVEL")
    level = int(raw_level) if raw_level else None
    if kind == "gzip" and level is None:
        level = 1
    return ExportConfig(
        load_postgres_config(values),
        paths,
        registry_path(values),
        runs,
        int(setting("RANGE_WIDTH", "100000")),
        resource_caps(values),
        int(setting("SHARD_BYTES", str(128 * 1024**2))),
        snapshot,
        kind,
        level,
    )


def load_index_config(environ: Mapping[str, str], env_file: Path | None = None) -> IndexConfig:
    """Load QLever runtime settings with capped index and query memory."""
    from shutil import which

    values = load_qlever_environment(environ, env_file)
    paths = load_data_paths({}, values)

    def setting(key: str, default: str = "") -> str:
        """Read a QLever-prefixed setting or its default."""
        return values.get("STACK2GRAPH_QLEVER_" + key, default)

    limits = detect_limits()
    budget = resource_budget(limits)
    index_budget = cap_budget(budget, memory_bytes=int(setting("INDEX_RAM_BYTES", str(budget.memory_bytes))))
    query_budget = cap_budget(budget, memory_bytes=int(setting("QUERY_RAM_BYTES", "1073741824")))
    executable = setting("EXECUTABLE", which("qlever") or "qlever")
    return IndexConfig(
        Path(values.get("STACK2GRAPH_GRAPH_ARTIFACT_ROOT", str(paths.data_root / "graph-runs"))).resolve(),
        Path(setting("INDEX_ROOT", str(paths.data_root / "graph-indexes"))).resolve(),
        QLeverSettings(
            Path(executable),
            setting("IMAGE"),
            setting("HOST", "127.0.0.1"),
            int(setting("PORT", "7015")),
            index_budget.memory_bytes,
            query_budget.memory_bytes,
            int(setting("TIMEOUT_SECONDS", "3600")),
            index_timeout_seconds=int(setting("INDEX_TIMEOUT_SECONDS", str(24 * 60 * 60))),
            network=setting("NETWORK"),
            server_name=setting("CONTAINER_NAME"),
            compose_project=setting("COMPOSE_PROJECT", "stack2graph-qlever"),
        ),
    )


def load_qlever_environment(environ: Mapping[str, str], env_file: Path | None = None) -> dict[str, str]:
    """Load application and QLever settings without other service configuration."""
    values = load_environment({}, env_file)
    service_env = REPOSITORY / "deployment" / "qlever" / ".env"
    if env_file is None and service_env.exists():
        values.update(
            {key: value for key, value in read_env(service_env).items() if key.startswith("STACK2GRAPH_QLEVER_")}
        )
    values.update(environ)
    return values
