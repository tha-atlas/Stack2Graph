"""
Upload shards through several independent Qdrant loader processes.

A single Python loader process tops out near 900 points/s.
"""

import multiprocessing as mp
from multiprocessing.process import BaseProcess
from pathlib import Path
from threading import Event

from vector.domain.config import QdrantConfig
from vector.domain.loading import LoadSettings
from vector.loading.load import run_load
from vector.shards import Manifest, RunLayout
from vector.workers import guarded_worker, parent_process_alive, supervise_processes


def run_load_worker(
    index: int, run_root: str, manifest: Manifest, qdrant: QdrantConfig, settings: LoadSettings, follow: bool
) -> None:
    """Load claimed shards in a spawned process with its own Qdrant client."""
    from vector.adapter.qdrant_store import QdrantVectorStore

    layout = RunLayout(Path(run_root))
    parent_alive = parent_process_alive()

    def body() -> None:
        """Load claimed shards and close the worker's Qdrant client on exit."""
        store = QdrantVectorStore(qdrant)
        try:
            run_load(layout, manifest, store, settings, follow=follow, parent_alive=parent_alive)
        finally:
            store.close()

    guarded_worker(layout, f"load{index}", body)


def run_loaders(
    layout: RunLayout,
    manifest: Manifest,
    qdrant: QdrantConfig,
    settings: LoadSettings,
    processes: int,
    *,
    follow: bool,
    stop: Event | None = None,
) -> None:
    """Spawn and supervise the configured number of shard-loading processes."""
    if processes <= 0:
        raise ValueError("At least one loader process is required")
    context = mp.get_context("spawn")
    workers: list[tuple[str, BaseProcess]] = [
        (
            f"load{index}",
            context.Process(
                target=run_load_worker,
                args=(index, str(layout.root), manifest, qdrant, settings, follow),
                name=f"load{index}",
            ),
        )
        for index in range(processes)
    ]
    supervise_processes(layout, workers, stop=stop)
