"""Select completed graph or vector runs using shared ranking rules."""

import logging
from collections.abc import Callable
from pathlib import Path

from utils.types import JSONObject
from validation.domain.models import SnapshotTarget
from validation.domain.report import ArtifactKind, Observations, validated_for_target

LOGGER = logging.getLogger(__name__)


def select_completed_run(
    root: Path,
    *,
    completed: Callable[[Path], Path | None],
    features: Callable[[str], tuple[JSONObject, str | None]],
    observations: Observations,
    artifact: ArtifactKind,
    target: SnapshotTarget,
    preferred_registry: str | None,
) -> str | None:
    """
    Select the best completed run by validation, registry, date, and name.

    Prior validation for the target ranks first, then a compatible selection
    registry, then the newest completion marker, then the run name. Older
    source provenance never excludes a candidate; candidates whose evidence
    cannot be read are warned about and skipped. Candidate enumeration and
    ordering stay private to this module; callers pass observation loading and
    evidence reading as callbacks.
    """
    if not root.is_dir():
        return None
    best: tuple[int, int, float, str] | None = None
    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        completion = completed(entry)
        if completion is None:
            continue
        marker_mtime = completion.stat().st_mtime
        try:
            binding, registry = features(entry.name)
        except (OSError, ValueError) as error:
            LOGGER.warning("Ignoring unreadable %s candidate %s: %s", artifact, entry.name, error)
            continue
        prior_validated = validated_for_target(
            observations,
            artifact=artifact,
            run=entry.name,
            binding=binding,
            target_snapshot_id=target.snapshot_id,
        )
        compatible = preferred_registry is None or registry == preferred_registry
        rank = (0 if prior_validated else 1, 0 if compatible else 1, -marker_mtime, entry.name)
        if best is None or rank < best:
            best = rank
    return best[3] if best is not None else None
