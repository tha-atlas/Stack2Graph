"""Run verified Stack Overflow snapshot preparation from the command line."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypeAlias

if TYPE_CHECKING:
    from sql.domain.config import PipelineConfig
    from sql.interfaces.pipeline import Downloader
    from sql.services.pipeline import DumpPipeline
    from utils.config import DataPaths
    from utils.resources import ResourceCaps
    from utils.types import JSONObject

DownloadMethod: TypeAlias = Literal["torrent", "http"]


class Arguments(argparse.Namespace):
    command: str | None
    snapshot: str
    data_root: str | None
    data_dir: str | None
    xml_path: str | None
    postgres_path: str | None
    env_file: Path | None
    seven_zip: str | None
    aria2c: str | None
    download_method: DownloadMethod | None
    stop_after: str | None
    threads: int | None
    memory_bytes: int | None
    batch_rows: int | None
    batch_bytes: int | None
    record_bytes: int | None
    spill_bytes: int | None
    disk_reserve: int | None
    json: bool
    log_level: str
    overwrite: bool


def positive_int(value: str) -> int:
    """Parse a positive integer or raise an argument validation error."""
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("Value must be positive")
    return result


def _select_download_method(
    parser: argparse.ArgumentParser, arguments: Arguments, method: DownloadMethod | None
) -> DownloadMethod:
    """Choose download/run transfer via a flag or prompt; require a choice."""
    if method is None:
        if not os.isatty(0):
            parser.error("--download-method torrent or http is required without an interactive terminal")
        while method is None:
            print("Download method [torrent/http]: ", end="", file=sys.stderr, flush=True)
            try:
                choice = input().strip().lower()
            except EOFError:
                parser.error("No download method selected; use --download-method torrent or http")
            if choice == "torrent":
                method = "torrent"
            elif choice == "http":
                method = "http"
            else:
                print("Please enter torrent or http.", file=sys.stderr)
    if method == "torrent":
        from sql.archive.torrent import ARIA2C_UNAVAILABLE, aria2c_executable

        if aria2c_executable(arguments.aria2c) is None:
            raise RuntimeError(ARIA2C_UNAVAILABLE)
    return method


def _resource_caps(arguments: Arguments) -> ResourceCaps:
    """Apply CLI overrides while retaining unspecified ResourceCaps defaults."""
    from utils.resources import ResourceCaps

    defaults = ResourceCaps()
    return ResourceCaps(
        threads=arguments.threads,
        memory_bytes=arguments.memory_bytes,
        batch_rows=arguments.batch_rows if arguments.batch_rows is not None else defaults.batch_rows,
        batch_bytes=arguments.batch_bytes,
        record_bytes=arguments.record_bytes,
        spill_bytes=arguments.spill_bytes if arguments.spill_bytes is not None else defaults.spill_bytes,
        disk_reserve=arguments.disk_reserve if arguments.disk_reserve is not None else defaults.disk_reserve,
    )


def _build_pipeline(
    arguments: Arguments,
    paths: DataPaths,
    config: PipelineConfig,
    caps: ResourceCaps,
    env_file: Path | None,
    method: DownloadMethod | None,
) -> DumpPipeline:
    """Assemble required adapters with lazy database and validation setup."""
    from sql.adapter.importer import PostgreSQLStoreFactory
    from sql.archive.download import ArchiveDownloader
    from sql.archive.extract import SevenZipExtractor
    from sql.archive.source import ArchiveSource, HTTPClient
    from sql.archive.torrent import TorrentDownloader
    from sql.config import load_postgres_config
    from sql.services.pipeline import DumpPipeline
    from sql.services.validation import build_publication_validator
    from utils.config import load_environment

    client = HTTPClient()
    downloader: Downloader | None = None
    if method == "torrent":
        downloader = TorrentDownloader(client, client, binary=arguments.aria2c, disk_reserve=caps.disk_reserve)
    elif method == "http":
        downloader = ArchiveDownloader(client, disk_reserve=caps.disk_reserve)
    store_active = arguments.command in ("import", "validate", "run") and arguments.stop_after not in (
        "download",
        "extract",
    )
    return DumpPipeline(
        config,
        ArchiveSource(client, cache_dir=paths.data_dir / "discovery"),
        downloader,
        SevenZipExtractor(arguments.seven_zip),
        PostgreSQLStoreFactory(load_postgres_config(os.environ, env_file)) if store_active else None,
        publication_validator=(
            build_publication_validator(paths.data_dir, load_environment(os.environ, env_file))
            if store_active
            else None
        ),
    )


def _print_result(result: JSONObject, json_output: bool) -> None:
    """Print one pipeline result in the requested text or JSON format."""
    from utils.types import json_array, json_object

    if json_output:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    elif "snapshots" in result:
        for entry in json_array(result["snapshots"]):
            snapshot = json_object(entry)
            print(
                f"{snapshot['item_id']}: {snapshot.get('stage', 'not selected')}; writer_active={snapshot['writer_active']}",
            )
        if not result["snapshots"]:
            print("No local snapshots.")
    else:
        print(f"{result['item_id']}: {result.get('stage', 'not selected')}; writer_active={result['writer_active']}")
        print(result["progress_note"])


def main(argv: list[str] | None = None) -> None:
    """Parse SQL pipeline arguments, run the command, and print its result."""
    parser = argparse.ArgumentParser(
        description="Download, extract, import and validate Stack Overflow dump snapshots.",
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=("download", "extract", "import", "validate", "run", "status"),
    )
    parser.add_argument("--snapshot", default="latest", help="IA item ID; latest resolves once on a new selection")
    for name in ("data-root", "data-dir", "xml-path", "postgres-path"):
        parser.add_argument(f"--{name}")
    parser.add_argument("--env-file", type=Path, help="Literal environment file; default: existing local .env")
    parser.add_argument("--seven-zip", help="Checked native 7-Zip 26.04+ binary, preferably 7zz")
    parser.add_argument("--aria2c", help="Path to the optional aria2c torrent client")
    parser.add_argument(
        "--download-method",
        choices=("torrent", "http"),
        help="required for download/run; prompts in a terminal if omitted; no automatic fallback",
    )
    parser.add_argument("--stop-after", choices=("download", "extract", "import", "validate"))
    parser.add_argument("--overwrite", action="store_true", help="Rebuild existing PostgreSQL output from XML")
    for name in ("threads", "memory-bytes", "batch-rows", "batch-bytes", "record-bytes", "spill-bytes", "disk-reserve"):
        parser.add_argument(f"--{name}", type=positive_int)
    parser.add_argument("--json", action="store_true", help="Print machine-readable local status")
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
        help="Console log level; run files retain application DEBUG details",
    )
    arguments = parser.parse_args(argv, namespace=Arguments())
    if arguments.command is None:
        parser.error("command is required; use --help")
    from utils.logging_utils import get_logger, setup_logging

    setup_logging(
        level={"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING, "ERROR": logging.ERROR}[
            arguments.log_level
        ],
    )
    logger = get_logger(__name__)
    from sql.domain.config import PipelineConfig
    from sql.parsing.worker_selection import load_xml_workers
    from sql.services.pipeline import DumpPipeline
    from utils.config import REPOSITORY, load_data_paths

    try:
        local_env = REPOSITORY / ".env"
        env_file = (
            arguments.env_file
            if arguments.env_file is not None
            else (local_env if local_env.exists() or local_env.is_symlink() else None)
        )
        overrides = {
            "data_root": arguments.data_root,
            "data_dir": arguments.data_dir,
            "xml_path": arguments.xml_path,
            "postgres_path": arguments.postgres_path,
        }
        paths = load_data_paths(
            {name: value for name, value in overrides.items() if value is not None},
            os.environ,
            env_file,
        )
        method = arguments.download_method
        if arguments.command in ("download", "run"):
            method = _select_download_method(parser, arguments, method)
        caps = _resource_caps(arguments)
        config = PipelineConfig(paths, caps, xml_workers=load_xml_workers(os.environ, env_file))
        pipeline = (
            DumpPipeline(config, None, None, None, None)
            if arguments.command == "status"
            else _build_pipeline(arguments, paths, config, caps, env_file, method)
        )
        if arguments.overwrite:
            result = pipeline.execute(arguments.command, arguments.snapshot, arguments.stop_after, overwrite=True)
        else:
            result = pipeline.execute(arguments.command, arguments.snapshot, arguments.stop_after)
    except (ValueError, RuntimeError, OSError) as error:
        logger.error("%s", error)
        logger.debug("Command traceback", exc_info=True)
        parser.exit(2)
    except KeyboardInterrupt:
        logger.warning("Interrupted; rerun the failed stage explicitly, or use run --overwrite for a full rebuild")
        parser.exit(130)
    _print_result(result, arguments.json)


if __name__ == "__main__":
    if __package__ is None:
        import sys

        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    main()
