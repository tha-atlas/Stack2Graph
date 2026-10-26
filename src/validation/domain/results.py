"""Represent individual comparisons and the results of each validation stage."""

from dataclasses import dataclass
from typing import Literal

from utils.types import JSONObject

ComparisonKind = Literal["count", "identity", "configuration", "prerequisite"]
ComparisonOutcome = Literal["passed", "failed", "skipped"]
ComparisonReason = Literal[
    "count-mismatch",
    "identity-conflict",
    "config-conflict",
    "prerequisite-unavailable",
    "outdated",
]
ComparisonSystem = Literal["postgres", "vector", "graph", "qdrant"]


@dataclass(frozen=True)
class Comparison:
    """Record a named count, identity, configuration, or prerequisite comparison."""

    check: str
    kind: ComparisonKind
    outcome: ComparisonOutcome
    reason: ComparisonReason | None = None
    system: ComparisonSystem | None = None
    expected: int | None = None
    actual: int | None = None
    unit: str | None = None
    expected_identity: str | None = None
    observed_identity: str | None = None

    @property
    def passed(self) -> bool:
        """Whether this comparison passed."""
        return self.outcome == "passed"

    @property
    def difference(self) -> int | None:
        """Signed count difference (actual - expected); None for non-count results."""
        if self.expected is None or self.actual is None:
            return None
        return self.actual - self.expected

    def to_json(self) -> JSONObject:
        """Serialize the comparison, identities, and count difference."""
        return {
            "check": self.check,
            "kind": self.kind,
            "outcome": self.outcome,
            "reason": self.reason,
            "system": self.system,
            "expected": self.expected,
            "actual": self.actual,
            "difference": self.difference,
            "unit": self.unit,
            "expected_identity": self.expected_identity,
            "observed_identity": self.observed_identity,
        }


def count_comparison(
    check: str,
    *,
    expected: int,
    actual: int,
    unit: str,
    system_on_mismatch: ComparisonSystem | None = None,
) -> Comparison:
    """Compare two counts; attribute a mismatch only where the evidence supports it."""
    if expected == actual:
        return Comparison(check=check, kind="count", outcome="passed", expected=expected, actual=actual, unit=unit)
    return Comparison(
        check=check,
        kind="count",
        outcome="failed",
        reason="count-mismatch",
        system=system_on_mismatch,
        expected=expected,
        actual=actual,
        unit=unit,
    )


def envelope_comparison(check: str, *, limit: int, actual: int, unit: str) -> Comparison:
    """Compare a subset count against its upper bound; actual must not exceed limit."""
    if actual <= limit:
        return Comparison(check=check, kind="count", outcome="passed", expected=limit, actual=actual, unit=unit)
    return Comparison(
        check=check,
        kind="count",
        outcome="failed",
        reason="count-mismatch",
        expected=limit,
        actual=actual,
        unit=unit,
    )


def _literal_comparison(
    check: str, *, kind: ComparisonKind, conflict: ComparisonReason, expected: str, observed: str
) -> Comparison:
    """Compare identifiers, reporting the supplied reason for a conflict."""
    if expected == observed:
        return Comparison(check=check, kind=kind, outcome="passed", expected_identity=expected)
    return Comparison(
        check=check,
        kind=kind,
        outcome="failed",
        reason=conflict,
        expected_identity=expected,
        observed_identity=observed,
    )


def identity_comparison(check: str, *, expected: str, observed: str) -> Comparison:
    """Compare identifiers and label any mismatch as an identity conflict."""
    return _literal_comparison(
        check, kind="identity", conflict="identity-conflict", expected=expected, observed=observed
    )


def configuration_comparison(check: str, *, expected: str, observed: str) -> Comparison:
    """Compare settings and label any mismatch as a configuration conflict."""
    return _literal_comparison(
        check, kind="configuration", conflict="config-conflict", expected=expected, observed=observed
    )


def prerequisite_comparison(check: str, *, satisfied: bool, system: ComparisonSystem | None = None) -> Comparison:
    """Record counterpart validation and attribute missing evidence to it."""
    if satisfied:
        return Comparison(check=check, kind="prerequisite", outcome="passed")
    return Comparison(
        check=check,
        kind="prerequisite",
        outcome="failed",
        reason="prerequisite-unavailable",
        system=system,
    )


def skipped_comparison(check: str, *, reason: ComparisonReason) -> Comparison:
    """Record a skipped prerequisite check with its reason."""
    return Comparison(check=check, kind="prerequisite", outcome="skipped", reason=reason)


@dataclass(frozen=True)
class CheckResults:
    """Collect every comparison made by one check during an invocation."""

    stage: str
    comparisons: tuple[Comparison, ...]

    @property
    def passed(self) -> bool:
        """Whether every comparison passed."""
        return all(item.passed for item in self.comparisons)

    @property
    def failures(self) -> tuple[Comparison, ...]:
        """The comparisons whose outcome is failed."""
        return tuple(item for item in self.comparisons if item.outcome == "failed")

    @property
    def skipped(self) -> tuple[Comparison, ...]:
        """The comparisons whose outcome is skipped."""
        return tuple(item for item in self.comparisons if item.outcome == "skipped")

    def to_json(self) -> JSONObject:
        """Serialize stage comparisons and their combined outcome."""
        outcome: ComparisonOutcome = "passed"
        if self.failures:
            outcome = "failed"
        elif self.skipped:
            outcome = "skipped"
        return {
            "stage": self.stage,
            "outcome": outcome,
            "comparisons": [item.to_json() for item in self.comparisons],
        }
