"""Run selected vector stages with dependencies loaded only when needed."""

import argparse
import sys
from pathlib import Path


class Arguments(argparse.Namespace):
    env_file: Path | None
    command: str
    run: str
    after_id: int | None
    limit_questions: int | None
    overwrite: bool


def main(argv: list[str] | None = None) -> int:
    """Run the requested vector stage and return a command exit status."""
    parser = argparse.ArgumentParser(description="Build question vectors in Qdrant.")
    parser.add_argument("command", choices=("extract", "embed", "load", "finalize", "all"))
    parser.add_argument("--run", required=True, help="Run name; files live under STACK2GRAPH_VECTOR_RUN_DIR/<run>.")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--after-id", type=int, help="Scan questions after this ID (first extract of a run only).")
    parser.add_argument("--limit-questions", type=int, help="Scan at most this many questions (first extract only).")
    parser.add_argument("--overwrite", action="store_true", help="Clear this run's files and rebuild all stages")
    arguments = parser.parse_args(argv, namespace=Arguments())
    import os

    from utils.config import load_environment
    from utils.logging_utils import get_logger, setup_logging
    from vector.config import load_run_config
    from vector.preparation.extract import FIRST_ID
    from vector.progress import DEPENDENCY_CONSOLE_LEVELS
    from vector.services.runner import RunRequest, StageName, install_termination_handlers, run_stage
    from vector.services.validation import build_finalization_validator

    setup_logging(console_logger_levels=DEPENDENCY_CONSOLE_LEVELS)
    install_termination_handlers()
    stages: dict[str, StageName] = {
        "extract": "extract",
        "embed": "embed",
        "load": "load",
        "finalize": "finalize",
        "all": "all",
    }
    try:
        config = load_run_config(os.environ, arguments.env_file)
    except Exception as error:
        get_logger(__name__).error("Vector build configuration failed (%s): %s", type(error).__name__, error)
        return 1
    request = RunRequest(
        stages[arguments.command],
        arguments.run,
        FIRST_ID if arguments.after_id is None else arguments.after_id,
        arguments.limit_questions,
        arguments.overwrite,
    )
    try:
        run_stage(
            config,
            request,
            validation=build_finalization_validator(config, load_environment(os.environ, arguments.env_file)),
        )
    except Exception:
        # The run context has already recorded the error and its traceback.
        return 1
    return 0


if __name__ == "__main__":
    if __package__ in (None, ""):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    raise SystemExit(main())
