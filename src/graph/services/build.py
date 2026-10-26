"""Export, index, verify, and publish a graph through the managed service."""

from hashlib import sha256
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

from graph.adapter.qlever import QLeverAdapter
from graph.deployment.service import ManagedGraphService
from graph.domain.artifacts import ArtifactLayout
from graph.domain.config import ExportConfig
from graph.domain.index import IndexConfig, IndexSelection
from graph.services.export import GraphExporter
from graph.services.index import select_artifacts
from graph.services.query_checks import verify_candidate
from utils.artifacts import fsync_directory
from utils.logging_utils import RunLog, current_run, get_logger, pipeline_run
from utils.manifest import SnapshotLock

LOGGER = get_logger(__name__)


class GraphBuilder:
    def __init__(
        self,
        export: ExportConfig,
        index: IndexConfig,
        service: ManagedGraphService,
    ) -> None:
        """Bind export configuration, index configuration, and the managed service."""
        self.export, self.index, self.service = export, index, service

    def run(self, run: str, *, resume: bool = False) -> str:
        """Build and publish a graph index, reusing an active run log if present."""
        active = current_run()
        if active is not None:
            return self._build(run, resume=resume, log=active)
        with pipeline_run("build", log_dir=self.export.paths.data_root / "graph-reports", reuse_active=True) as log:
            return self._build(run, resume=resume, log=log)

    def _build(self, run: str, *, resume: bool, log: RunLog) -> str:
        """Export, build or reuse an index, verify it, and publish the service."""
        LOGGER.info("Checking QLever service")
        self.service.prepare_build()
        manifest = GraphExporter(self.export).run(run, resume=resume)
        languages = manifest.language_keys
        selected = select_artifacts(ArtifactLayout(self.export.run_root / run), IndexSelection(languages))
        directory = self.index.index_root / selected.input_digest[:20]
        runtime = QLeverAdapter(self.index.settings)
        try:
            with SnapshotLock(self.index.index_root / ("build-" + selected.input_digest[:20])):
                for abandoned in self.index.index_root.glob(".building-" + directory.name + "-*"):
                    self._discard(runtime, abandoned)
                if directory.exists():
                    index = runtime.inspect(directory)
                    if index.input_digest != selected.input_digest or index.identity != selected.identity:
                        raise ValueError("Existing managed index differs from the completed export")
                else:
                    temporary = directory.with_name(".building-" + directory.name + "-" + uuid4().hex)
                    try:
                        with log.stage("index"):
                            runtime.build(selected, temporary, published_path=directory)
                        temporary.rename(directory)
                        fsync_directory(directory.parent)
                    finally:
                        if temporary.exists():
                            self._discard(runtime, temporary)
                with log.stage("verify"):
                    verify_candidate(self.index.settings, directory, instance_key="build-verify-" + directory.name)
                with log.stage("publish"):
                    return self.service.publish(directory)
        finally:
            rmtree(selected.shard_catalog.parent)

    @staticmethod
    def _discard(runtime: QLeverAdapter, directory: Path) -> None:
        """Stop an owned staging index container and remove its directory."""
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError("Managed index staging path must be an owned directory without symlinks")
        identity = sha256(str(directory).encode()).hexdigest()[:20]
        runtime.stop_named("stack2graph-kg-index-" + identity, directory)
        rmtree(directory)
