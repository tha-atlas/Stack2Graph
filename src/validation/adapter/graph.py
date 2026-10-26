"""Read processed source-row counts from completed graph manifests."""

from pathlib import Path

from utils.types import JSONObject, JSONValue, json_array, json_integer, json_object, json_string, parse_json
from validation.domain.models import GraphArtifactEvidence, TableCounts

PROCESSED_COUNTS_KEY = "processed_table_counts"


def graph_completed(run: Path) -> Path | None:
    """Return a completed graph run's manifest path, or None."""
    path = run / "manifest.json"
    if not path.is_file():
        return None
    try:
        document = json_object(parse_json(path.read_text(encoding="utf-8")))
    except OSError, ValueError:
        return None
    return path if document.get("complete") is True else None


def _required(document: JSONObject, key: str, *, run: str) -> JSONValue:
    """Raise ValueError when a required manifest contract field is absent."""
    if key not in document:
        raise ValueError(f"Graph run {run} manifest lacks {key!r}")
    return document[key]


def _processed_table_counts(value: JSONValue) -> TableCounts:
    """Decode the export contract: one [table, count] pair per processed source table."""
    rows: list[tuple[str, int]] = []
    for entry in json_array(value):
        pair = json_array(entry)
        if len(pair) != 2:
            raise ValueError(f"Processed table count entry {pair!r} is not a [table, count] pair")
        rows.append((json_string(pair[0]), json_integer(pair[1])))
    return tuple(rows)


class GraphArtifactAdapter:
    """Read final graph counts without opening range catalogs or RDF files."""

    def __init__(self, run_directory: Path, *, run: str) -> None:
        """Store the graph run directory and run name for evidence reads."""
        self._directory = run_directory
        self._run = run

    def evidence(self) -> GraphArtifactEvidence:
        """Read completed graph evidence, rejecting missing contract fields."""
        path = self._directory / "manifest.json"
        if not path.is_file():
            raise ValueError(f"Graph run {self._run} has no manifest at {path}")
        document = json_object(parse_json(path.read_text(encoding="utf-8")))
        if document.get("complete") is not True:
            raise ValueError(f"Graph run {self._run} manifest is not marked complete")
        identity = json_object(_required(document, "identity", run=self._run))
        inventory = json_object(_required(document, "inventory", run=self._run))
        if PROCESSED_COUNTS_KEY not in document:
            raise ValueError(
                f"Graph run {self._run} manifest lacks {PROCESSED_COUNTS_KEY!r}; "
                "export it with a graph pipeline that publishes processed source-row counts"
            )
        return GraphArtifactEvidence(
            run=self._run,
            source_snapshot_id=json_string(_required(identity, "snapshot_id", run=self._run)),
            source_snapshot_digest=json_string(_required(identity, "snapshot_digest", run=self._run)),
            registry_digest=json_string(_required(identity, "registry_digest", run=self._run)),
            exporter_digest=json_string(_required(identity, "exporter_digest", run=self._run)),
            exported_questions=json_integer(_required(inventory, "question_count", run=self._run)),
            processed_table_counts=_processed_table_counts(document[PROCESSED_COUNTS_KEY]),
        )
