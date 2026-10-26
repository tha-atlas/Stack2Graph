"""Connect configuration and resources to the four vector processing stages."""

import os
import re
import signal
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from threading import Event, Thread
from types import FrameType
from typing import Literal

from selection.registry import load_selection_registry
from sql.adapter.reader import PostgreSQLSnapshotReader
from utils.logging_utils import get_logger, pipeline_run
from utils.resources import BatchLimits, ResourcePlan, detect_limits, require_writable_directory
from utils.types import JSONObject
from vector.adapter.qdrant_store import QdrantVectorStore
from vector.domain.config import RunConfig
from vector.domain.embedding import EmbedSettings
from vector.domain.loading import LoadSettings
from vector.domain.models import CollectionState
from vector.embedding.model_files import DENSE_DIMENSION, DENSE_MODEL, SPARSE_MODEL, resolve_model_paths
from vector.loading.finalize import FinalizationValidator, run_finalize
from vector.loading.load import ensure_collection
from vector.loading.loaders import run_loaders
from vector.preparation.extract import FIRST_ID, initialize_worker, run_extract
from vector.preparation.source import VectorQuestionSource
from vector.progress import RunProgress, configure_embedding_output
from vector.retry import retry_operation
from vector.shards import (
    Manifest,
    RunLayout,
    check_manifest,
    release_stale_claims,
    remove_temporary,
    reset_run,
    run_lock,
)

LOGGER = get_logger(__name__)
StageName = Literal["extract", "embed", "load", "finalize", "all"]
_RUN_NAME = re.compile(r"[A-Za-z0-9_-]+")


@dataclass(frozen=True)
class RunRequest:
    stage: StageName
    run: str
    after_id: int = FIRST_ID
    question_limit: int | None = None
    overwrite: bool = False


def creates_collection(stage: StageName) -> bool:
    """Return whether a stage may recreate a confirmed upload collection."""
    return stage in ("load", "all")


def install_termination_handlers() -> None:
    """Convert SIGTERM/SIGHUP to SystemExit for stage and GPU cleanup."""

    def terminate(number: int, _frame: FrameType | None) -> None:
        """Exit with a signal-derived status so worker cleanup can run."""
        raise SystemExit(128 + number)

    for number in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(number, terminate)


def validate_run_name(name: str) -> None:
    """Reject run names containing characters outside the allowed set."""
    if not _RUN_NAME.fullmatch(name):
        raise ValueError("Run names may only contain letters, digits, underscores and hyphens")


def confirm_rebuild(state: CollectionState) -> bool:
    """Ask an interactive user to confirm the exact collection name."""
    if not os.isatty(0):
        return False
    print(f"Delete and rebuild Qdrant collection {state.name!r} ({state.point_count} points).")
    try:
        return input("Type the exact collection name to confirm: ") == state.name
    except EOFError:
        return False


def run_concurrently(tasks: Mapping[str, Callable[[Event], None]]) -> dict[str, float]:
    """Run stages in threads; stop peers and re-raise the first failure."""
    stop = Event()
    seconds: dict[str, float] = {}
    errors: list[BaseException] = []

    def run(name: str, task: Callable[[Event], None]) -> None:
        """Time a stage and record its failure before stopping sibling stages."""
        started = time.perf_counter()
        try:
            task(stop)
        except BaseException as error:
            if not stop.is_set():
                errors.append(error)
                LOGGER.error("Stage %s failed (%s): %s", name, type(error).__name__, error)
            stop.set()
        finally:
            seconds[name] = time.perf_counter() - started

    threads = [Thread(target=run, args=(name, task), name=f"stage-{name}") for name, task in tasks.items()]
    for thread in threads:
        thread.start()
    try:
        for thread in threads:
            thread.join()
    except BaseException:
        stop.set()
        for thread in threads:
            thread.join()
        raise
    if errors:
        raise errors[0]
    return seconds


def _source(config: RunConfig) -> PostgreSQLSnapshotReader:
    """Open a pinned snapshot reader with the vector pipeline's read limits."""
    limits = config.limits
    resources = ResourcePlan(
        limits.cpu_workers,
        limits.ram_bytes,
        limits.ram_bytes // 4,
        BatchLimits(limits.read_rows, limits.read_bytes, limits.read_bytes),
        1,
        1,
        detect_limits().evidence,
    )
    return PostgreSQLSnapshotReader(
        config.paths, resources, config.postgres, snapshot_id=config.snapshot, read_attempts=limits.attempts
    )


def _manifest(config: RunConfig, registry_digest: str, snapshot_digest: str, request: RunRequest) -> Manifest:
    """Build the run identity from source, registry, models, and chunk rules."""
    return Manifest(
        snapshot_id=config.snapshot,
        snapshot_digest=snapshot_digest,
        registry_digest=registry_digest,
        dense_model=DENSE_MODEL,
        dense_revision=config.inference.dense_revision,
        sparse_model=SPARSE_MODEL,
        sparse_revision=config.inference.sparse_revision,
        dense_dimension=DENSE_DIMENSION,
        chunk_tokens=config.chunks.max_tokens,
        chunk_overlap=config.chunks.overlap,
        collection=config.qdrant.collection,
        after_id=request.after_id,
        question_limit=request.question_limit,
    )


def _settings(config: RunConfig) -> JSONObject:
    """Return stage worker counts and batching settings for the report."""
    stages = config.stages
    return {
        "gpus": list(stages.gpus),
        "load_processes": stages.load_processes,
        "shard_children": stages.shard_children,
        "dense_batch_tokens": stages.dense_batch_tokens,
        "dense_batch_rows": stages.dense_batch_rows,
        "sparse_batch_size": stages.sparse_batch_size,
        "extract_workers": stages.extract_workers,
        "upload_workers": stages.upload_workers,
        "upload_points": stages.upload_points,
    }


def _prepare_source(
    config: RunConfig,
    registry_digest: str,
    request: RunRequest,
    layout: RunLayout,
    stack: ExitStack,
) -> tuple[PostgreSQLSnapshotReader | None, Manifest]:
    """Open extracting stages' reader and create or check the run manifest."""
    needs_source = request.stage in ("extract", "all") and not layout.extract_complete.exists()
    source = _source(config) if needs_source else None
    if source is not None:
        stack.callback(source.close)
    snapshot_digest = source.snapshot_identity.digest if source is not None else ""
    manifest = check_manifest(
        layout, _manifest(config, registry_digest, snapshot_digest, request), create=source is not None
    )
    return source, manifest


def _prepare_store(
    config: RunConfig, stage: StageName, layout: RunLayout, manifest: Manifest, stack: ExitStack
) -> QdrantVectorStore | None:
    """Connect and preflight upload/finalize targets before loading models."""
    if stage not in ("load", "finalize", "all"):
        return None
    store = QdrantVectorStore(config.qdrant)
    stack.callback(store.close)
    # Connectivity and target state are checked before any model loads.
    retry_operation(store.inspect, config.limits.attempts)
    if creates_collection(stage):
        ensure_collection(layout, store, manifest.dense_dimension, confirm_rebuild)
    return store


def run_stage(config: RunConfig, request: RunRequest, validation: FinalizationValidator | None = None) -> None:
    """
    Lock a vector run and execute the requested stage with scoped logging.

    Args:
        config: Source, model, collection, resource, and stage settings.
        request: Run name and stage, with optional source window or overwrite.
        validation: Optional finalization check before completion publication.

    Raises:
        ValueError: The run name or requested overwrite policy is invalid.

    """
    validate_run_name(request.run)
    if request.overwrite and request.stage != "all":
        raise ValueError("Overwrite applies only to the all stage")
    require_writable_directory(config.report_dir)
    config.stages.run_dir.mkdir(parents=True, exist_ok=True)
    layout = RunLayout(config.stages.run_dir / request.run)
    with pipeline_run("vectordatabase", log_dir=config.report_dir, run_logger=LOGGER), run_lock(layout):
        if request.stage == "all":
            reset_run(layout, overwrite=request.overwrite)
        _run_locked(config, request, layout, validation)


def _run_locked(
    config: RunConfig, request: RunRequest, layout: RunLayout, validation: FinalizationValidator | None
) -> None:
    """Prepare run resources and coordinate extraction, embedding, and loading."""
    layout.create()
    removed, released = remove_temporary(layout), release_stale_claims(layout)
    if removed or released:
        LOGGER.info("Removed %s partial files and %s stale claims from an earlier attempt", removed, released)
    registry = load_selection_registry(config.registry_path)
    stage = request.stage
    with ExitStack() as stack:
        source, manifest = _prepare_source(config, registry.digest, request, layout, stack)
        store = _prepare_store(config, stage, layout, manifest, stack)
        configure_embedding_output()
        models = resolve_model_paths(config.inference) if stage in ("extract", "embed", "all") else None
        stages = config.stages
        embed_settings = EmbedSettings(
            stages.gpus,
            stages.dense_batch_tokens,
            stages.dense_batch_rows,
            stages.sparse_batch_size,
            config.chunks.max_tokens,
            manifest.dense_dimension,
        )
        load_settings = LoadSettings(stages.upload_points, stages.upload_workers, config.limits.attempts)
        follow = stage == "all"

        def extract(stop: Event) -> None:
            """Extract selected questions with initialized process-pool workers."""
            if source is None or models is None:
                return
            with ProcessPoolExecutor(
                stages.extract_workers,
                initializer=initialize_worker,
                initargs=(config.registry_path, models.dense, models.sparse, config.chunks, manifest.snapshot_digest),
            ) as executor:
                run_extract(
                    layout,
                    VectorQuestionSource(source),
                    executor,
                    shard_children=stages.shard_children,
                    read_rows=config.limits.read_rows,
                    after_id=manifest.after_id,
                    question_limit=manifest.question_limit,
                    stop=stop,
                )

        def embed(stop: Event) -> None:
            """Run GPU embedding workers with the shared stop event."""
            from vector.embedding.gpu import run_embed

            run_embed(layout, _require(models), embed_settings, follow=follow, stop=stop)

        def load(stop: Event) -> None:
            """Run shard-loading processes with the shared stop event."""
            run_loaders(layout, manifest, config.qdrant, load_settings, stages.load_processes, follow=follow, stop=stop)

        tasks: dict[str, Callable[[Event], None]] = {}
        if stage in ("extract", "all"):
            tasks["extract"] = extract
        if stage in ("embed", "all"):
            tasks["embed"] = embed
        if stage in ("load", "all"):
            tasks["load"] = load
        with RunProgress(layout).track():
            started = time.monotonic()
            seconds = run_concurrently(tasks) if tasks else {}
            if stage != "all":
                seconds[stage] = time.monotonic() - started
            if stage in ("finalize", "all"):
                # For `all`, finalize measures the total wall time itself, including indexing and readiness.
                report = run_finalize(
                    layout,
                    _require(store),
                    config.qdrant.finalize_timeout,
                    seconds,
                    _settings(config),
                    started=started if stage == "all" else None,
                    manifest=manifest,
                    validation=validation,
                )
                LOGGER.info("Run %s finished: %s points; report %s", layout.root.name, report["points"], layout.report)


def _require[Value](value: Value | None) -> Value:
    """Return a prepared resource or raise if it is missing."""
    if value is None:
        raise RuntimeError("A required stage resource was not prepared")
    return value
