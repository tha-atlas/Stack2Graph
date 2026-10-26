"""Compare counts from pipelines or CLI calls through independent checks."""

import logging

from utils.logging_utils import ProgressReporter, stage_event
from validation.domain.models import (
    IMPORTED_TABLES,
    GraphArtifactEvidence,
    QdrantCollectionEvidence,
    TableCounts,
    ValidationMismatchError,
    VectorArtifactEvidence,
)
from validation.domain.results import (
    CheckResults,
    Comparison,
    configuration_comparison,
    count_comparison,
    envelope_comparison,
    identity_comparison,
    prerequisite_comparison,
)
from validation.domain.stages import GRAPH_POSTGRES, GRAPH_QDRANT, POSTGRES_QDRANT, XML_POSTGRES

LOGGER = logging.getLogger(__name__)


def _table_counts(stage: str, side: str, counts: TableCounts) -> dict[str, int]:
    """Require exactly one count per imported table before any comparison runs."""
    known = dict(counts)
    missing = [table for table in IMPORTED_TABLES if table not in known]
    unexpected = [table for table in known if table not in IMPORTED_TABLES]
    if len(counts) != len(known) or missing or unexpected:
        raise ValueError(
            f"{stage}: {side} counts must cover exactly {', '.join(IMPORTED_TABLES)} once each"
            f" (missing: {missing or 'none'}, unexpected: {unexpected or 'none'})"
        )
    return known


def _finish(results: CheckResults) -> CheckResults:
    """Log check completion and raise on failed comparisons."""
    stage_event("validation pass complete", stage=results.stage, checked=len(results.comparisons))
    if results.failures:
        raise ValidationMismatchError(results.stage, results)
    return results


def _compare_tables(
    stage: str,
    *,
    expected: TableCounts,
    actual: TableCounts,
    total: int,
) -> list[Comparison]:
    """Compare expected and actual counts for every imported table."""
    reporter = ProgressReporter(LOGGER, stage, unit="rows", total=total)
    expected_by_table = _table_counts(stage, "expected", expected)
    actual_by_table = _table_counts(stage, "actual", actual)
    comparisons = [
        count_comparison(
            table,
            expected=expected_by_table[table],
            actual=actual_by_table[table],
            unit="rows",
        )
        for table in IMPORTED_TABLES
    ]
    reporter.update(total, force=True)
    return comparisons


def validate_xml_postgres(*, xml_counts: TableCounts, postgres_counts: TableCounts) -> CheckResults:
    """
    Compare independent XML parser counts with exact PostgreSQL counts per table.

    Two counters from different stages cannot prove which side is wrong, so a
    mismatch is never attributed to one system.
    """
    stage = XML_POSTGRES
    stage_event("validation pass start", stage=stage, tables=len(IMPORTED_TABLES))
    results = CheckResults(
        stage=stage,
        comparisons=tuple(
            _compare_tables(stage, expected=xml_counts, actual=postgres_counts, total=len(IMPORTED_TABLES))
        ),
    )
    return _finish(results)


def validate_graph_postgres(*, expected_counts: TableCounts, graph: GraphArtifactEvidence) -> CheckResults:
    """
    Compare filtered source counts with processed graph rows.

    Expected counts use the exporter's own filters against the pinned target;
    processed counts come only from records the export handled successfully.
    """
    stage = GRAPH_POSTGRES
    stage_event("validation pass start", stage=stage, tables=len(IMPORTED_TABLES))
    results = CheckResults(
        stage=stage,
        comparisons=tuple(
            _compare_tables(
                stage, expected=expected_counts, actual=graph.processed_table_counts, total=len(IMPORTED_TABLES)
            )
        ),
    )
    return _finish(results)


def validate_postgres_qdrant(
    *,
    vector: VectorArtifactEvidence,
    postgres_window_questions: int,
    qdrant: QdrantCollectionEvidence,
) -> CheckResults:
    """
    Check source-window counts, Qdrant identity, and finalized points.

    A collection count differing from finalized vector evidence attributes to
    Qdrant; window-count and identity conflicts prove no single system wrong.
    """
    stage = POSTGRES_QDRANT
    stage_event("validation pass start", stage=stage, questions=postgres_window_questions)
    reporter = ProgressReporter(LOGGER, stage, unit="questions", total=max(1, postgres_window_questions))
    comparisons = (
        count_comparison(
            "vector.scanned_questions",
            expected=postgres_window_questions,
            actual=vector.scanned_questions,
            unit="questions",
        ),
        identity_comparison("qdrant.collection", expected=vector.collection, observed=qdrant.collection),
        identity_comparison("qdrant.build", expected=vector.build_id, observed=qdrant.build_id),
        configuration_comparison(
            "qdrant.dense-dimension",
            expected=str(vector.dense_dimension),
            observed=str(qdrant.dense_dimension),
        ),
        count_comparison(
            "qdrant.points",
            expected=vector.finalized_points,
            actual=qdrant.point_count,
            unit="points",
            system_on_mismatch="qdrant",
        ),
    )
    reporter.update(max(1, postgres_window_questions), force=True)
    return _finish(CheckResults(stage=stage, comparisons=comparisons))


def validate_graph_qdrant(
    *,
    graph: GraphArtifactEvidence,
    vector: VectorArtifactEvidence,
    qdrant: QdrantCollectionEvidence,
    graph_validated_for_target: bool,
    vector_validated_for_target: bool,
) -> CheckResults:
    """
    Check shared validation, registry, subset, and graph/Qdrant counts.

    Prerequisites are attributed to the counterpart that lacks its validation
    for the target. Configuration and envelope conflicts prove the pairing wrong
    without blaming one artifact; Qdrant point-count mismatches attribute to
    Qdrant so the graph and vector artifacts keep their independently validated
    state.
    """
    stage = GRAPH_QDRANT
    stage_event("validation pass start", stage=stage, questions=vector.selected_questions)
    reporter = ProgressReporter(LOGGER, stage, unit="points", total=vector.finalized_points)
    graph_prerequisite = prerequisite_comparison(
        "graph.target-validation", satisfied=graph_validated_for_target, system="graph"
    )
    comparisons: list[Comparison] = [graph_prerequisite]
    if graph_prerequisite.outcome == "failed":
        return _finish(CheckResults(stage=stage, comparisons=tuple(comparisons)))
    vector_prerequisite = prerequisite_comparison(
        "vector.target-validation", satisfied=vector_validated_for_target, system="vector"
    )
    comparisons.append(vector_prerequisite)
    if vector_prerequisite.outcome == "failed":
        return _finish(CheckResults(stage=stage, comparisons=tuple(comparisons)))

    comparisons.append(
        configuration_comparison(
            "selection.registry",
            expected=graph.registry_digest,
            observed=vector.registry_digest,
        )
    )
    if comparisons[-1].outcome == "failed":
        return _finish(CheckResults(stage=stage, comparisons=tuple(comparisons)))

    comparisons.append(
        envelope_comparison(
            "vector.selected-questions",
            limit=graph.exported_questions,
            actual=vector.selected_questions,
            unit="questions",
        )
    )
    comparisons.append(
        count_comparison(
            "qdrant.points",
            expected=vector.finalized_points,
            actual=qdrant.point_count,
            unit="points",
            system_on_mismatch="qdrant",
        )
    )
    comparisons.append(identity_comparison("qdrant.collection", expected=vector.collection, observed=qdrant.collection))
    reporter.update(vector.finalized_points, force=True)
    return _finish(CheckResults(stage=stage, comparisons=tuple(comparisons)))
