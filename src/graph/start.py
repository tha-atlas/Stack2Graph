"""Run graph commands while keeping help and artifact reads database-free."""

import argparse
import os
import sys
from pathlib import Path
from typing import BinaryIO, Literal


class Arguments(argparse.Namespace):
    command: str
    env_file: Path | None
    run: str | None
    artifacts: Path | None
    catalog: Path | None
    languages: list[str] | None
    candidate: Path | None
    first_id: int | None
    end_id: int | None
    resume: bool


def _build_command(args: Arguments) -> None:
    """Load configuration, build the requested graph run, and print its endpoint."""
    if args.run is None:
        raise ValueError("build requires --run")
    from graph.config import deployment_root, load_export_config, load_index_config, load_qlever_environment
    from graph.deployment.service import ManagedGraphService
    from graph.services.build import GraphBuilder

    values = load_qlever_environment(os.environ, args.env_file)
    index = load_index_config(values, args.env_file)
    export = load_export_config(values, args.env_file)
    service = ManagedGraphService(index.settings, deployment_root(index, values), index.index_root)
    builder = GraphBuilder(export, index, service)
    print(builder.run(args.run, resume=args.resume))


def _publish_command(args: Arguments) -> None:
    """Publish the requested verified candidate and print its endpoint."""
    if args.candidate is None:
        raise ValueError("publish requires --candidate")
    from graph.config import deployment_root, load_index_config, load_qlever_environment
    from graph.deployment.service import ManagedGraphService

    values = load_qlever_environment(os.environ, args.env_file)
    config = load_index_config(values, args.env_file)
    service = ManagedGraphService(config.settings, deployment_root(config, values), config.index_root)
    print(service.publish(args.candidate.resolve()))


def _benchmark_command(args: Arguments) -> None:
    """Benchmark an explicit sample range across worker and compression cases."""
    if args.first_id is None or args.end_id is None:
        raise ValueError("benchmark requires an explicit --first-id/--end-id sample range")
    from graph.config import load_export_config
    from graph.domain.source import QuestionRange
    from graph.services.benchmark import BenchmarkCase, GraphBenchmark
    from utils.resources import detect_limits, resource_budget

    config = load_export_config(os.environ, args.env_file)
    limit = resource_budget(
        detect_limits(), threads=config.limits.threads, memory_bytes=config.limits.memory_bytes
    ).threads
    counts: list[int] = []
    workers = 1
    while workers <= limit:
        counts.append(workers)
        workers *= 2
    modes: tuple[tuple[Literal["none", "gzip"], int | None], ...] = (
        ("none", None),
        ("gzip", 1),
        ("gzip", 3),
        ("gzip", 6),
    )
    cases = tuple(
        BenchmarkCase(workers, rows, compression, level)
        for workers in counts
        for rows in (256, 1024, 4096)
        for compression, level in modes
    )
    benchmark = GraphBenchmark(config)
    benchmark.run(QuestionRange(args.first_id, args.end_id), cases)
    print(benchmark.report_path)


def _index_command(args: Arguments) -> None:
    """Build an index from selected artifacts or verify a supplied candidate."""
    from graph.adapter.qlever import QLeverAdapter
    from graph.config import load_index_config
    from graph.domain.artifacts import ArtifactLayout
    from graph.domain.index import IndexSelection
    from graph.services.index import select_artifacts
    from graph.services.validate import read_manifest

    config = load_index_config(os.environ, args.env_file)
    adapter = QLeverAdapter(config.settings)
    if args.command == "index":
        layout = ArtifactLayout((args.artifacts or config.artifact_root).resolve())
        manifest = read_manifest(layout, require_complete=True)
        languages = tuple(args.languages) if args.languages is not None else manifest.language_keys
        selected = select_artifacts(layout, IndexSelection(languages))
        candidate = args.candidate or config.index_root / selected.input_digest[:20]
        adapter.build(selected, candidate.resolve())
    else:
        if args.candidate is None:
            raise ValueError("verify requires --candidate")
        from graph.services.query_checks import verify_candidate

        verify_candidate(config.settings, args.candidate.resolve())


def main(argv: list[str] | None = None) -> int:
    """Run a graph subcommand with scoped logging and return its exit status."""
    parser = argparse.ArgumentParser(description="Export and index Stack Overflow named knowledge graphs.")
    parser.add_argument(
        "command",
        choices=("build", "export", "resume", "stream", "index", "verify", "publish", "benchmark", "rollback"),
    )
    parser.add_argument("--env-file", type=Path, help="Optional alternative to repository-root .env")
    parser.add_argument("--run")
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--catalog", type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--languages", nargs="*")
    parser.add_argument("--first-id", type=int)
    parser.add_argument("--end-id", type=int)
    parser.add_argument("--resume", action="store_true", help="Resume a managed build after interruption")
    args = parser.parse_args(argv, namespace=Arguments())
    from utils.config import load_data_paths, load_environment
    from utils.logging_utils import get_logger, pipeline_run, setup_logging

    setup_logging()
    try:
        if args.command in ("build", "rollback", "index", "verify", "publish"):
            from graph.config import load_qlever_environment

            values = load_qlever_environment(os.environ, args.env_file)
        else:
            values = load_environment(os.environ, args.env_file)
        paths = load_data_paths({}, values)
        with pipeline_run("graph", log_dir=paths.data_root / "graph-reports"):
            _dispatch(args)
    except (ValueError, RuntimeError, OSError, NotImplementedError) as error:
        get_logger(__name__).error("Graph command failed: %s", error)
        return 1
    return 0


def _dispatch(args: Arguments) -> None:
    """Dispatch the parsed command to its graph workflow."""
    if args.command == "build":
        _build_command(args)
    elif args.command == "publish":
        _publish_command(args)
    elif args.command == "rollback":
        from graph.config import deployment_root, load_index_config, load_qlever_environment
        from graph.deployment.service import ManagedGraphService

        values = load_qlever_environment(os.environ, args.env_file)
        config = load_index_config(values, args.env_file)
        ManagedGraphService(config.settings, deployment_root(config, values), config.index_root).rollback()
    elif args.command == "stream":
        if args.catalog is None:
            raise ValueError("stream requires --catalog")
        from graph.adapter.catalog_stream import stream_catalog

        output: BinaryIO = sys.stdout.buffer
        stream_catalog(args.catalog.resolve(), output)
    elif args.command == "benchmark":
        _benchmark_command(args)
    elif args.command in ("export", "resume"):
        if args.run is None:
            raise ValueError("export requires --run")
        from graph.config import load_export_config
        from graph.services.export import GraphExporter

        GraphExporter(load_export_config(os.environ, args.env_file)).run(args.run, resume=args.command == "resume")
    else:
        _index_command(args)


if __name__ == "__main__":
    if __package__ in (None, ""):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    raise SystemExit(main())
