"""Check XML and PostgreSQL counts before publishing a SQL snapshot."""

from collections.abc import Mapping
from pathlib import Path

from sql.domain.models import SnapshotIdentity
from sql.interfaces.pipeline import PublicationValidator
from validation import validate_xml_postgres
from validation.domain.models import SnapshotTarget, TableCounts
from validation.domain.stages import XML_POSTGRES
from validation.services.embedded import EmbeddedCheck, embedded_validation_paths, run_embedded_validation


def build_publication_validator(data_root: Path, environ: Mapping[str, str]) -> PublicationValidator:
    """
    Gate every publication on the public `xml-postgres` count check.

    Each invocation writes exactly one report under the shared validation run
    root and raises on any per-table mismatch, so the pipeline refuses the
    durable publication marker while the committed rows stay untouched. The
    pinned target always comes from the pipeline's own snapshot identity, never
    from the environment.
    """
    run_root = embedded_validation_paths(data_root, environ).run_root

    def validate(identity: SnapshotIdentity, xml_counts: TableCounts, postgres_counts: TableCounts) -> None:
        """Run the blocking XML/database count check and publish its report."""
        run_embedded_validation(
            run_root=run_root,
            command="postgres",
            target=SnapshotTarget(snapshot_id=identity.item_id, snapshot_digest=identity.digest),
            checks=[
                EmbeddedCheck(
                    XML_POSTGRES,
                    blocking=True,
                    execute=lambda: validate_xml_postgres(xml_counts=xml_counts, postgres_counts=postgres_counts),
                    details={"xml-counts": "durable import progress"},
                )
            ],
        )

    return validate
