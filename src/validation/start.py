"""Parse validation commands without opening databases or artifact files."""

import argparse
import os
import sys
from pathlib import Path


class Arguments(argparse.Namespace):
    command: str
    run: str | None
    snapshot: str | None
    graph_run: str | None
    vector_run: str | None
    env_file: Path | None


def parse_arguments(argv: list[str] | None = None) -> Arguments:
    """Parse one of the four validation commands; no runtime resources are touched."""
    parser = argparse.ArgumentParser(
        prog="validation",
        description=(
            "Stage-oriented count validation: postgres (XML vs PostgreSQL tables), "
            "vector (PostgreSQL window vs Qdrant via the finalized vector run), "
            "graph (graph export vs PostgreSQL counts), "
            "run (all stages against one pinned snapshot target)."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def with_env_file(sub: argparse.ArgumentParser) -> argparse.ArgumentParser:
        """Add an optional alternative environment file to a subcommand."""
        sub.add_argument("--env-file", type=Path, help="Optional alternative to repository-root .env")
        return sub

    postgres = with_env_file(
        subparsers.add_parser("postgres", help="Compare XML parser counts with exact PostgreSQL counts")
    )
    postgres.add_argument("--snapshot", help="Pin the published PostgreSQL snapshot target")

    vector = with_env_file(
        subparsers.add_parser("vector", help="Validate the finalized vector run against PostgreSQL and Qdrant")
    )
    vector.add_argument("--vector-run", help="Pin the completed vector run")
    vector.add_argument("--graph-run", help="Optional graph counterpart for the count-envelope check")

    graph = with_env_file(
        subparsers.add_parser("graph", help="Validate the completed graph export against PostgreSQL counts")
    )
    graph.add_argument("--graph-run", help="Pin the completed graph run")
    graph.add_argument("--vector-run", help="Optional vector counterpart for the count-envelope check")

    run = with_env_file(subparsers.add_parser("run", help="Run every stage and publish one report"))
    run.add_argument("--run", help="Name for this validation run (generated when omitted)")
    run.add_argument("--snapshot", help="Pin the published PostgreSQL snapshot target")
    run.add_argument("--graph-run", help="Pin the completed graph run")
    run.add_argument("--vector-run", help="Pin the completed vector run")
    return parser.parse_args(argv, namespace=Arguments())


def main(argv: list[str] | None = None) -> int:
    """Run the selected validation command and return its exit status."""
    arguments = parse_arguments(argv)
    from validation.config import load_validation_config
    from validation.domain.models import ValidationRequest
    from validation.services.run import ValidationScope, run_validation

    scopes: dict[str, ValidationScope] = {"postgres": "postgres", "vector": "vector", "graph": "graph", "run": "run"}
    try:
        config = load_validation_config(os.environ, arguments.env_file)
        document = run_validation(
            config,
            ValidationRequest(
                run=arguments.run,
                snapshot=arguments.snapshot,
                graph_run=arguments.graph_run,
                vector_run=arguments.vector_run,
            ),
            scopes[arguments.command],
        )
    except Exception as error:
        print(f"validation failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    name = str(document["validation_run"])
    seconds = document["total_seconds"]
    elapsed = f"{seconds:.1f}" if isinstance(seconds, (int, float)) else "?"
    systems = document["systems"]
    summary = ", ".join(f"{key}={state}" for key, state in systems.items()) if isinstance(systems, dict) else ""
    line = f"validation {name} {document['outcome']} in {elapsed}s ({summary}); report: {name}/report.json"
    if document["outcome"] == "passed":
        print(line)
        return 0
    print(line, file=sys.stderr)
    return 1


if __name__ == "__main__":
    if __package__ in (None, ""):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    raise SystemExit(main())
