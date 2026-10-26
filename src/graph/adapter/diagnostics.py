"""Write bounded source-error reports without retaining or logging body HTML."""

from pathlib import Path

from graph.domain.artifacts import ArtifactLayout
from graph.domain.config import RunIdentity
from graph.domain.source import SourceIntegrityError, SourceProblem
from sql.domain.read_errors import SnapshotRecordBudgetError
from utils.artifacts import atomic_write_json
from utils.types import JSONObject, JSONValue


def write_failure(layout: ArtifactLayout, identity: RunIdentity | None, part: str | None, error: Exception) -> None:
    """Write bounded source or processing diagnostics for a run or part."""
    if isinstance(error, SourceIntegrityError):
        problems = error.problems[:64]
    elif isinstance(error, SnapshotRecordBudgetError):
        problems = (SourceProblem("record_budget", error.table, error.source_id, error.field, None, str(error)),)
    else:
        problems = (SourceProblem("processing_failure", "Graph", 0, "record", None, str(error)[:4096]),)
    errors: list[JSONValue] = [
        {
            "code": p.code,
            "table": p.table,
            "source_id": p.source_id,
            "field": p.field,
            "target_id": p.target_id,
            "message": p.message[:4096],
        }
        for p in problems
    ]
    value: JSONObject = {
        "complete": False,
        "identity": identity.to_json() if identity else None,
        "part": part,
        "errors": errors,
    }
    path: Path = layout.path("failure.json" if part is None else f"diagnostics/{part}.json")
    atomic_write_json(path, value)
