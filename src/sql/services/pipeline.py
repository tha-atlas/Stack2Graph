"""Check stage prerequisites and resume work through explicit dependencies."""

import shutil
from contextlib import closing
from dataclasses import replace
from time import monotonic

from sql.archive.download import ArchiveVerification
from sql.archive.extract import binary_json, read_inventory
from sql.archive.source import ArchiveSource, PublisherEvidence
from sql.archive.torrent import TORRENT_STAGING_SYMLINK, torrent_remaining_bytes
from sql.domain.config import PipelineConfig
from sql.domain.models import ArchiveChecksum, SnapshotIdentity, XMLInventory
from sql.domain.schema import TABLES
from sql.interfaces.pipeline import Downloader, Extractor, PublicationValidator, SnapshotSource, StoreFactory
from sql.parsing.parallel_xml import ParallelXMLTableStream, import_concurrency
from sql.parsing.worker_selection import XMLWorkerProfile
from utils.config import SnapshotPaths
from utils.logging_utils import ProgressReporter, RunLog, format_bytes, get_logger, pipeline_run, stage_event
from utils.manifest import ManifestStore, SnapshotLock, writer_active
from utils.resources import (
    ResourceCaps,
    ResourcePlan,
    detect_limits,
    plan_resources,
    require_disk,
    require_writable_directory,
)
from utils.types import JSONObject, json_integer, json_object

LOGGER = get_logger(__name__)
COMMANDS = ("download", "extract", "import", "validate", "run", "status")
STAGES = ("download", "extract", "import", "validate")


def _discard_incomplete_transfers(paths: SnapshotPaths) -> None:
    """Remove partial HTTP and torrent state unless an archive is published."""
    if paths.archive.is_symlink():
        raise ValueError("Archive path must not be a symlink")
    if paths.archive.is_file():
        return
    partial = paths.archive.with_suffix(".7z.part")
    transfer_state = paths.archive_dir / "transfer.json"
    torrent_work = paths.archive_dir / "torrent"
    for path in (partial, transfer_state):
        if path.is_symlink():
            raise ValueError(f"Transfer state must not be a symlink: {path}")
        path.unlink(missing_ok=True)
    if torrent_work.is_symlink():
        raise ValueError(TORRENT_STAGING_SYMLINK)
    if torrent_work.exists():
        shutil.rmtree(torrent_work)


def _resource_plan(caps: ResourceCaps) -> ResourcePlan:
    """Build a scalable plan from detected resource limits and configured caps."""
    return plan_resources(detect_limits(), caps, scalable=True)


def _require_inventory_archive(inventory: XMLInventory, archive: ArchiveChecksum | None) -> None:
    """Require the verified archive to match the XML inventory's archive."""
    if archive is not None and inventory.archive_checksum != archive:
        raise ValueError("XML inventory belongs to a different archive")


def read_status(paths: SnapshotPaths) -> JSONObject:
    """Read local snapshot state without connecting or creating storage."""
    state = ManifestStore(paths.manifest).read()
    if state and state.get("backend") != "postgresql":
        raise ValueError("Unsupported snapshot manifest; only PostgreSQL snapshot state is accepted")
    return {
        **state,
        "item_id": paths.item_id,
        "writer_active": writer_active(paths.state),
        "progress_note": "A full run starts PostgreSQL import from zero; standalone import can continue committed batches.",
    }


class DumpPipeline:
    def __init__(
        self,
        config: PipelineConfig,
        source: SnapshotSource | None,
        downloader: Downloader | None,
        extractor: Extractor | None,
        store_factory: StoreFactory | None,
        *,
        publication_validator: PublicationValidator | None = None,
    ) -> None:
        """Store stage dependencies and the optional publication validator."""
        self.config, self.source, self.downloader = config, source, downloader
        self.extractor, self.store_factory = extractor, store_factory
        self.publication_validator = publication_validator

    def _database_preflight(self, paths: SnapshotPaths, resources: ResourcePlan, state: JSONObject) -> None:
        """Check database readiness and add runtime versions to the mutable state."""
        if self.store_factory is None:
            raise RuntimeError("Database store is unavailable")
        LOGGER.info("Check PostgreSQL connectivity before source verification")
        try:
            state["runtime"] = list(self.store_factory.preflight(paths, resources))
        except ImportError as error:
            raise RuntimeError(
                f"PostgreSQL import/validation dependencies are missing: {error}. Set up the project environment first."
            ) from error

    def _preflight(
        self,
        paths: SnapshotPaths,
        identity: SnapshotIdentity,
        stages: tuple[str, ...],
        resources: ResourcePlan,
        state: JSONObject,
        manifest: ManifestStore,
        archive_verification: ArchiveVerification,
    ) -> None:
        """
        Check stage prerequisites before transfer and defer size checks as
        needed.
        """
        needs_database = any(stage in stages for stage in ("import", "validate"))
        if needs_database:
            self._database_preflight(paths, resources, state)
        directories = {paths.state}
        if "download" in stages and not paths.archive.exists():
            if self.downloader is None:
                raise RuntimeError("Archive downloader is unavailable")
            if self.source is None:
                raise RuntimeError("Snapshot source is unavailable for the remaining download")
            directories.add(paths.archive_dir)
            partial = paths.archive.with_suffix(".7z.part")
            if partial.is_symlink():
                raise ValueError("Partial archive symlinks are forbidden")
            remaining = (
                max(0, identity.size - partial.stat().st_size)
                if partial.is_file()
                else torrent_remaining_bytes(identity, paths)
            )
            require_disk(paths.archive_dir, remaining, resources.disk_reserve)
        if "extract" in stages:
            if paths.xml.exists():
                inventory = read_inventory(paths, identity)
                archive = archive_verification.verify() if paths.archive.exists() else None
                _require_inventory_archive(inventory, archive)
            else:
                if self.extractor is None:
                    raise RuntimeError("Archive extractor is unavailable")
                binary = self.extractor.preflight()
                if binary is not None:
                    state["seven_zip"] = binary_json(binary)
                directories.add(paths.xml.parent)
                require_disk(paths.xml.parent, 0, resources.disk_reserve)
        if needs_database:
            if self.store_factory is None:
                raise RuntimeError("Database store is unavailable")
            directories.add(paths.database_dir)
            require_disk(paths.database_dir, 0, resources.disk_reserve)
        for directory in sorted(directories):
            require_writable_directory(directory)
        manifest.write(state)

    def _recorded_item(self, requested: str) -> str:
        """Resolve local latest only when exactly one snapshot is recorded."""
        if requested != "latest":
            return requested
        root = self.config.paths.data_dir / "snapshots"
        recorded = sorted(path.parent.name for path in root.glob("*/manifest.json")) if root.exists() else []
        if len(recorded) != 1:
            raise ValueError("Local latest is missing or ambiguous; select a recorded --snapshot item ID")
        return recorded[0]

    def status(self, requested: str) -> JSONObject:
        """Read one or all local snapshot statuses without opening the database."""
        if requested != "latest":
            return read_status(SnapshotPaths.from_config(self.config.paths, requested))
        root = self.config.paths.data_dir / "snapshots"
        return {
            "snapshots": [
                read_status(SnapshotPaths.from_config(self.config.paths, path.parent.name))
                for path in sorted(root.glob("*/manifest.json"))
            ]
            if root.exists()
            else [],
        }

    def _resources(self, state: JSONObject, manifest: ManifestStore) -> ResourcePlan:
        """Plan resources and persist them in the supplied manifest state."""
        resources = _resource_plan(self.config.caps)
        state["resources"] = resources.to_json()
        manifest.write(state)
        LOGGER.debug(
            "Resource plan: threads=%s memory_bytes=%s database_bytes=%s evidence=%s",
            resources.threads,
            resources.memory_bytes,
            resources.database_bytes,
            resources.evidence,
        )
        return resources

    def _prepare_locked(
        self,
        command: str,
        stages: tuple[str, ...],
        overwrite: bool,
        paths: SnapshotPaths,
        identity: SnapshotIdentity,
        archive_verification: ArchiveVerification,
        manifest: ManifestStore,
    ) -> tuple[JSONObject, bool]:
        """
        Validate and complete the recorded snapshot state while the lock is held.

        Rejects unsupported manifests, foreign identities and unrequested database
        rebuilds, discards stale transfer state, bootstraps a first run from any
        pre-existing archive/XML, and records source provenance. Returns the working
        state plus the database-rebuild flag; the caller owns the lock and stages.
        """
        state = manifest.read()
        if state and state.get("backend") != "postgresql":
            raise ValueError(
                "Unsupported snapshot manifest; prepare new PostgreSQL snapshot state without modifying archives/XML"
            )
        if (
            state.get("identity")
            and SnapshotIdentity.from_dict(json_object(state["identity"])).digest != identity.digest
        ):
            raise ValueError("Snapshot already records a different source identity")
        has_database_output = (
            bool(state.get("progress"))
            or state.get("stage") == "published"
            or bool(state.get("publication"))
            or paths.publication.exists()
        )
        rebuild_database = command == "run" and "import" in stages and overwrite
        if has_database_output and command == "run" and "import" in stages and not overwrite:
            raise ValueError("PostgreSQL output already exists; pass --overwrite to rebuild from the XML inputs")
        if command in ("download", "run"):
            _discard_incomplete_transfers(paths)
        if not state:
            if any(stage in stages for stage in ("import", "validate")) and (
                paths.archive.exists() or paths.xml.exists()
            ):
                self._database_preflight(paths, _resource_plan(self.config.caps), state)
            archive = archive_verification.verify() if paths.archive.exists() else None
            if paths.xml.exists():
                _require_inventory_archive(read_inventory(paths, identity), archive)
            elif archive is None and paths.publication.exists():
                raise ValueError("Unlabelled existing inventory; do not adopt or overwrite it")
            state.update(identity=identity.to_json(), backend="postgresql", stage="selected")
            manifest.write(state)
        metadata = self.source.metadata.get(identity.item_id) if self.source is not None else None
        if metadata:
            ManifestStore(paths.state / "source-metadata.json").write(metadata)
            evidence = (
                self.source.evidence.documents
                if isinstance(self.source, ArchiveSource) and isinstance(self.source.evidence, PublisherEvidence)
                else {}
            )
            ManifestStore(paths.state / "source-evidence.json").write(
                {"identity_evidence": dict(identity.evidence), "documents": evidence},
            )
        return state, rebuild_database

    def _import(
        self,
        paths: SnapshotPaths,
        identity: SnapshotIdentity,
        state: JSONObject,
        manifest: ManifestStore,
        resources: ResourcePlan,
        run: RunLog,
    ) -> None:
        """Import XML batches with durable progress and close the store on exit."""
        inventory = read_inventory(paths, identity)
        initial_estimate = 0 if state.get("progress") else sum(x.size for x in inventory.inputs)
        require_disk(paths.database_dir, initial_estimate, resources.disk_reserve)
        if self.store_factory is None:
            raise RuntimeError("Database store is unavailable")
        concurrency = import_concurrency(resources)
        writer_resources = replace(
            resources, threads=max(1, resources.threads - concurrency.workers - (1 if concurrency.workers else 0))
        )
        LOGGER.info(
            "Import budgets: %s CPU threads, up to %s XML workers, %s writer threads, %s RAM",
            resources.threads,
            concurrency.workers,
            writer_resources.threads,
            format_bytes(resources.memory_bytes),
        )
        store = self.store_factory(paths, identity, inventory, writer_resources)
        try:
            progress_state: JSONObject = {table: store.progress(table).to_json() for table in TABLES}
            state["progress"] = progress_state
            manifest.write(state)
            for source in inventory.inputs:
                progress = store.progress(source.table)
                if progress.eof:
                    LOGGER.info("%s already complete: %s rows", source.table, f"{progress.count:,}")
                    continue
                LOGGER.info("Import %s from committed XML position %s", source.table, f"{progress.position:,}")
                with ProgressReporter(LOGGER, source.table, unit="rows", initial=progress.count).track() as reporter:
                    profile = (
                        XMLWorkerProfile(self.config.paths.data_dir / "xml-worker-profiles", source.table)
                        if self.config.xml_workers is None
                        else None
                    )
                    stream = ParallelXMLTableStream(
                        source,
                        resources,
                        progress.position,
                        encoder=store.batch_encoder,
                        workers=self.config.xml_workers,
                        profile=profile,
                    )
                    last_manifest = monotonic()
                    batch_count, write_seconds = 0, 0.0
                    with closing(iter(stream)) as batches:
                        for batch in batches:
                            started = monotonic()
                            progress = store.commit_encoded_batch(batch)
                            write_seconds += monotonic() - started
                            batch_count += 1
                            progress_state[source.table] = progress.to_json()
                            if monotonic() - last_manifest >= 5.0:
                                manifest.write(state)
                                last_manifest = monotonic()
                            reporter.update(progress.count)
                    if stream.xml_rows is None:
                        raise RuntimeError("XML stream did not report its completed row count")
                    progress = store.finish_table(source.table, stream.xml_rows)
                    progress_state[source.table] = progress.to_json()
                    manifest.write(state)
                    reporter.update(progress.count, force=True)
                    LOGGER.info(
                        "%s import timings: %s batches; %.2fs in writer/DB commits; selected XML concurrency %s",
                        source.table,
                        batch_count,
                        write_seconds,
                        stream.tuner.width,
                    )
                    stage_event(
                        "table_completed", table=source.table, rows=progress.count, xml_position=progress.position
                    )
            state["stage"] = "imported"
            manifest.write(state)
            run.summary(
                "Imported rows",
                {table: json_integer(json_object(value)["count"]) for table, value in progress_state.items()},
            )
        finally:
            store.close()

    def _publish(
        self,
        paths: SnapshotPaths,
        identity: SnapshotIdentity,
        resources: ResourcePlan,
    ) -> None:
        """Validate and publish the snapshot, then close the store."""
        inventory = read_inventory(paths, identity)
        if self.store_factory is None:
            raise RuntimeError("Database store is unavailable")
        store = self.store_factory(paths, identity, inventory, resources)
        try:
            if self.publication_validator is None:
                store.publish()
                return
            # One COUNT(*) scan (validate) feeds both sides of the public check: the
            # report carries the ledger's XML counters beside the actual database
            # counts, so a diverging import counter reaches the gate and blocks.
            report = store.validate()
            self.publication_validator(identity, report.xml_counts, report.counts)
            store.publish(report)
        finally:
            store.close()

    def execute(
        self,
        command: str,
        requested_snapshot: str = "latest",
        stop_after: str | None = None,
        *,
        overwrite: bool = False,
    ) -> JSONObject:
        """
        Run selected snapshot stages under a lock and persist their state.

        Args:
            command: Pipeline stage, run, or local status command.
            requested_snapshot: Item ID, or latest for snapshot resolution.
            stop_after: Last stage to execute; supported only for run.
            overwrite: Allow run to rebuild existing snapshot output from XML.

        Returns:
            Local status or the persisted snapshot state after processing.

        Raises:
            ValueError: Command options or snapshot state are incompatible.
            RuntimeError: A selected stage fails; its error is saved in the manifest.

        """
        if command not in COMMANDS or stop_after not in (*STAGES, None):
            raise ValueError("Unsupported command or stop-after stage")
        if stop_after and command != "run":
            raise ValueError("--stop-after applies only to run")
        if overwrite and command != "run":
            raise ValueError("--overwrite applies only to run")
        if command == "status":
            return self.status(requested_snapshot)
        stages: tuple[str, ...] = STAGES if command == "run" else (command,)
        if stop_after:
            stages = stages[: stages.index(stop_after) + 1]
        if command in ("run", "download"):
            if self.source is None:
                raise RuntimeError("Snapshot source is unavailable")
            identity = self.source.resolve(requested_snapshot)
            paths = SnapshotPaths.from_config(self.config.paths, identity.item_id)
        else:
            paths = SnapshotPaths.from_config(self.config.paths, self._recorded_item(requested_snapshot))
            recorded = ManifestStore(paths.manifest).read().get("identity")
            if not recorded:
                raise ValueError("No recorded snapshot identity; run download first")
            identity = SnapshotIdentity.from_dict(json_object(recorded))
            if identity.item_id != paths.item_id:
                raise ValueError("Recorded item identity conflicts with snapshot directory")
        with SnapshotLock(paths.state):
            archive_verification = ArchiveVerification(paths.archive, identity)
            manifest = ManifestStore(paths.manifest)
            state, rebuild_database = self._prepare_locked(
                command, stages, overwrite, paths, identity, archive_verification, manifest
            )
            with pipeline_run(
                "dump",
                log_dir=paths.state,
                log_name="pipeline.log",
                run_logger=LOGGER,
                settings={
                    "snapshot": identity.item_id,
                    "command": command,
                    "release": identity.release_date,
                    "data_root": self.config.paths.data_root,
                },
            ) as run:
                current_stage = command
                try:
                    LOGGER.info("Data root: %s; snapshot: %s", self.config.paths.data_root, identity.item_id)
                    state["paths"] = {
                        "data_root": str(self.config.paths.data_root),
                        "data_dir": str(self.config.paths.data_dir),
                        "xml_root": str(self.config.paths.xml_root),
                        "postgres_root": str(self.config.paths.postgres_root),
                    }
                    resources = self._resources(state, manifest)
                    run.summary(
                        "Resource budget",
                        {
                            "Threads": resources.threads,
                            "Memory": format_bytes(resources.memory_bytes),
                            "PostgreSQL memory": format_bytes(resources.database_bytes),
                            "Batch rows": resources.batches.max_rows,
                            "Batch payload": format_bytes(resources.batches.max_bytes),
                        },
                    )
                    if (
                        state.get("publication")
                        and any(stage in stages for stage in ("import", "validate"))
                        and not rebuild_database
                    ):
                        # A published snapshot never re-imports through this invocation:
                        # standalone import/validate collapse onto the publication gate alone.
                        stages = ("validate",)
                        current_stage = "validate"
                    self._preflight(paths, identity, stages, resources, state, manifest, archive_verification)
                    published = state.get("stage") == "published" and not rebuild_database
                    for current_stage in stages:
                        with run.stage(current_stage):
                            if current_stage == "download":
                                if paths.archive.exists():
                                    result = archive_verification.verify()
                                else:
                                    if self.downloader is None:
                                        raise RuntimeError("Archive downloader is unavailable")
                                    if state.get("stage") != "selected":
                                        if self.source is None:
                                            raise RuntimeError(
                                                "Snapshot source is unavailable for the remaining download"
                                            )
                                        self.source.verify_recorded(identity)
                                    result = self.downloader.download(identity, paths)
                                state.update(
                                    stage="published" if published else "downloaded",
                                    archive_fingerprint=result.to_json(),
                                )
                                manifest.write(state)
                            elif current_stage == "extract":
                                if not paths.archive.is_file():
                                    raise RuntimeError("Verified archive missing; run download first")
                                archive = archive_verification.verify()
                                if paths.xml.exists():
                                    inventory = read_inventory(paths, identity)
                                elif self.extractor is not None:
                                    inventory = self.extractor.extract(identity, paths, resources, archive)
                                else:
                                    raise RuntimeError("Archive extractor is unavailable")
                                _require_inventory_archive(inventory, archive)
                                state.update(
                                    stage="published" if published else "extracted", inventory=inventory.digest
                                )
                                manifest.write(state)
                            elif current_stage == "import":
                                if command == "run":
                                    if self.store_factory is None:
                                        raise RuntimeError("Database store is unavailable")
                                    self.store_factory.reset(paths, identity, overwrite=rebuild_database)
                                if rebuild_database:
                                    paths.publication.unlink(missing_ok=True)
                                    for key in ("progress", "publication", "validation", "last_error"):
                                        state.pop(key, None)
                                    state.update(identity=identity.to_json(), backend="postgresql")
                                    state["stage"] = "extracted"
                                    manifest.write(state)
                                    rebuild_database = False
                                self._import(paths, identity, state, manifest, resources, run)
                            elif current_stage == "validate":
                                self._publish(paths, identity, resources)
                                state = manifest.read()
                        if stop_after == current_stage:
                            LOGGER.info("Stopped after %s; invoke the next stage explicitly to continue", current_stage)
                            break
                    return read_status(paths)
                except Exception as error:
                    # Reread publication projection; a failure must not replace newer committed evidence.
                    failed = manifest.read()
                    failed["last_error"] = {"stage": current_stage, "message": str(error)}
                    manifest.write(failed)
                    raise RuntimeError(f"{identity.item_id} stage {current_stage}: {error}") from error
