"""Read saved reports and derive the latest outcome per artifact and target."""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from utils.types import JSONObject, JSONValue, canonical_hash, json_array, json_integer, json_object, json_string
from validation.domain.models import IMPORTED_TABLES, SnapshotTarget, TableCounts
from validation.domain.stages import GRAPH_POSTGRES

REPORT_VERSION = "stage-count-validation-1"
REPORT_NAME = "report.json"

ArtifactKind = Literal["vector", "graph"]
_ARTIFACT_KINDS: dict[str, ArtifactKind] = {"vector": "vector", "graph": "graph"}


def _parse_outcome(value: JSONValue) -> Literal["passed", "failed"]:
    """Decode a passed or failed outcome, rejecting other values."""
    text_value = json_string(value)
    if text_value == "passed":
        return "passed"
    if text_value == "failed":
        return "failed"
    raise ValueError(f"Unknown validation observation outcome {text_value!r}")


def _parse_target(document: JSONObject) -> SnapshotTarget:
    """Read the target snapshot ID and digest from a report."""
    target = json_object(document["target"])
    return SnapshotTarget(
        snapshot_id=json_string(target["snapshot_id"]), snapshot_digest=json_string(target["snapshot_digest"])
    )


def _parse_expected_counts(document: JSONObject) -> TableCounts | None:
    """Recover expected counts from passed per-table stage comparisons."""
    stages = {json_string(json_object(stage)["stage"]): json_object(stage) for stage in json_array(document["stages"])}
    stage = stages.get(GRAPH_POSTGRES)
    if stage is None:
        return None
    expected: dict[str, int] = {}
    for item in json_array(stage["comparisons"]):
        comparison = json_object(item)
        check = json_string(comparison["check"])
        if check in IMPORTED_TABLES and json_string(comparison["outcome"]) == "passed":
            expected[check] = json_integer(comparison["expected"])
    if any(table not in expected for table in IMPORTED_TABLES):
        return None
    return tuple((table, expected[table]) for table in IMPORTED_TABLES)


@dataclass(frozen=True)
class ValidationObservation:
    """Record an artifact's latest reported outcome for one validation target."""

    artifact: ArtifactKind
    run: str
    binding: JSONObject
    target: SnapshotTarget
    outcome: Literal["passed", "failed"]
    validation_run: str
    completed_at: str
    expected_table_counts: TableCounts | None

    @property
    def key(self) -> tuple[str, str, str, str]:
        """The artifact identity and target snapshot used for deduplication."""
        return (self.artifact, self.run, canonical_hash(self.binding), self.target.snapshot_id)


ObservationKey = tuple[str, str, str, str]
Observations = dict[ObservationKey, ValidationObservation]


def _lookup_key(artifact: ArtifactKind, run: str, binding: JSONObject, target_snapshot_id: str) -> ObservationKey:
    """Build the artifact and target key used to group validation observations."""
    return (artifact, run, canonical_hash(binding), target_snapshot_id)


def _observation(document: JSONObject, artifact: JSONObject) -> ValidationObservation:
    """Decode one artifact observation and any graph table-count evidence."""
    kind_value = json_string(artifact["artifact"])
    kind = _ARTIFACT_KINDS.get(kind_value)
    if kind is None:
        raise ValueError(f"Report names unknown artifact kind {kind_value!r}")
    expected_counts: TableCounts | None = None
    if kind == "graph":
        expected_counts = _parse_expected_counts(document)
    return ValidationObservation(
        artifact=kind,
        run=json_string(artifact["run"]),
        binding=json_object(artifact["binding"]),
        target=_parse_target(document),
        outcome=_parse_outcome(artifact["outcome"]),
        validation_run=json_string(document["validation_run"]),
        completed_at=json_string(document["completed_at"]),
        expected_table_counts=expected_counts,
    )


def parse_observations(documents: Iterable[JSONObject]) -> Observations:
    """Keep the newest artifact/target observation, including newer failures."""
    ordered: list[JSONObject] = []
    for document in documents:
        if document.get("version") != REPORT_VERSION:
            continue
        if document.get("target") is None:
            continue
        ordered.append(document)
    ordered.sort(key=lambda doc: (json_string(doc["completed_at"]), json_string(doc["validation_run"])))
    latest: Observations = {}
    for document in ordered:
        artifacts = document.get("artifacts")
        if artifacts is None:
            continue
        for item in json_array(artifacts):
            observation = _observation(document, json_object(item))
            latest[observation.key] = observation
    return latest


def validated_for_target(
    observations: Observations,
    *,
    artifact: ArtifactKind,
    run: str,
    binding: JSONObject,
    target_snapshot_id: str,
) -> bool:
    """Check whether the newest exact artifact/target observation passed."""
    found = observations.get(_lookup_key(artifact, run, binding, target_snapshot_id))
    return found is not None and found.outcome == "passed"


def reusable_expected_counts(
    observations: Observations,
    *,
    run: str,
    binding: JSONObject,
    target: SnapshotTarget,
) -> TableCounts | None:
    """Read counts from the last passed graph/database check for this target."""
    found = observations.get(_lookup_key("graph", run, binding, target.snapshot_id))
    if found is None or found.outcome != "passed" or found.target != target:
        return None
    return found.expected_table_counts


def last_successful_reference(
    observations: Observations,
    *,
    artifact: ArtifactKind,
    run: str,
    binding: JSONObject,
    target_snapshot_id: str,
) -> JSONObject | None:
    """Find the last report validating this artifact for the selected target."""
    found = observations.get(_lookup_key(artifact, run, binding, target_snapshot_id))
    if found is None or found.outcome != "passed":
        return None
    return {"validation_run": found.validation_run, "completed_at": found.completed_at}
