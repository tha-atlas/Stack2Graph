"""Read and write persisted validation reports."""

from pathlib import Path

from utils.artifacts import atomic_write_json
from utils.types import JSONObject, json_object, parse_json
from validation.domain.report import REPORT_NAME


def load_reports(run_root: Path) -> tuple[JSONObject, ...]:
    """Parse stored reports, treating corruption as a configuration error."""
    documents: list[JSONObject] = []
    if not run_root.is_dir():
        return ()
    for entry_name in sorted(entry.name for entry in run_root.iterdir()):
        path = run_root / entry_name / REPORT_NAME
        if not path.is_file():
            continue
        try:
            documents.append(json_object(parse_json(path.read_text(encoding="utf-8"))))
        except (OSError, ValueError) as error:
            raise ValueError(f"Validation report {path} is unreadable: {error}") from error
    return tuple(documents)


def write_report(run_dir: Path, document: JSONObject) -> None:
    """Atomically write the validation report in the run directory."""
    atomic_write_json(run_dir / REPORT_NAME, document)
