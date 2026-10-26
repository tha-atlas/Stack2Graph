"""Measure bounded export samples with the regular source and range exporter."""

import time
from dataclasses import dataclass, replace
from pathlib import Path
from threading import Event, Thread
from typing import Literal
from uuid import uuid4

import psutil

from graph.adapter.inventory import iter_shards
from graph.adapter.postgres_source import PostgreSQLGraphSource
from graph.domain.artifacts import ArtifactLayout
from graph.domain.config import ExportConfig
from graph.domain.source import DatabaseCapacity, QuestionRange
from graph.mapping.identity import IriPolicy
from graph.resources import GraphResourcePlanner
from graph.services.export import build_identity
from graph.services.parallel import ParallelExporter, RangeJob
from selection.languages import LanguageSelector
from selection.registry import load_selection_registry
from utils.artifacts import atomic_write_json
from utils.logging_utils import get_logger, pipeline_run
from utils.resources import detect_limits
from utils.types import JSONObject


@dataclass(frozen=True)
class BenchmarkCase:
    workers: int
    batch_rows: int
    compression: Literal["none", "gzip"]
    compression_level: int | None

    def to_json(self) -> JSONObject:
        """Serialize requested worker, batching, and compression settings."""
        return {
            "workers": self.workers,
            "batch_rows": self.batch_rows,
            "compression": self.compression,
            "compression_level": self.compression_level,
        }


@dataclass(frozen=True)
class BenchmarkResult:
    case: BenchmarkCase
    questions: int
    elapsed_seconds: float
    peak_rss_bytes: int
    written_bytes: int
    content_hashes: tuple[str, ...]
    successful: bool
    stored_hashes: tuple[str, ...] = ()
    error: str | None = None
    effective_case: BenchmarkCase | None = None

    def to_json(self) -> JSONObject:
        """Serialize benchmark timing, memory, hashes, effective settings, and errors."""
        return {
            "case": self.case.to_json(),
            "effective_case": self.effective_case.to_json() if self.effective_case is not None else None,
            "questions": self.questions,
            "elapsed_seconds": self.elapsed_seconds,
            "peak_rss_bytes": self.peak_rss_bytes,
            "written_bytes": self.written_bytes,
            "content_hashes": list(self.content_hashes),
            "stored_hashes": list(self.stored_hashes),
            "successful": self.successful,
            "error": self.error,
        }


class ProcessTreeMemoryProbe:
    def __init__(self) -> None:
        """Prepare a background sampler for this process tree's peak RSS."""
        self.peak_bytes = 0
        self._stop = Event()
        self._thread = Thread(target=self._run, daemon=True)

    def _sample(self) -> None:
        """Sample accessible process-tree RSS and update the peak byte count."""
        parent = psutil.Process()
        try:
            processes = (parent, *parent.children(recursive=True))
        except psutil.Error:
            processes = (parent,)
        rss = 0
        for process in processes:
            try:
                rss += process.memory_info().rss
            except psutil.Error:
                continue
        self.peak_bytes = max(self.peak_bytes, rss)

    def _run(self) -> None:
        """Sample process-tree memory until the stop event is set."""
        while not self._stop.is_set():
            self._sample()
            self._stop.wait(0.05)

    def start(self) -> None:
        """Start the background memory sampling thread."""
        self._thread.start()

    def stop(self) -> None:
        """Stop and join the sampling thread, then take a final memory sample."""
        self._stop.set()
        self._thread.join()
        self._sample()


class GraphBenchmark:
    def __init__(self, config: ExportConfig) -> None:
        """Store benchmark configuration and initialize report and artifact paths."""
        self.config = config
        self.report_path: Path | None = None
        self.case_roots: list[Path] = []

    def run(self, part: QuestionRange, cases: tuple[BenchmarkCase, ...]) -> tuple[BenchmarkResult, ...]:
        """Benchmark the requested question range with scoped run logging."""
        with pipeline_run("benchmark", log_dir=self.config.paths.data_root / "graph-reports", reuse_active=True):
            return self._benchmark(part, cases)

    def _benchmark(self, part: QuestionRange, cases: tuple[BenchmarkCase, ...]) -> tuple[BenchmarkResult, ...]:
        """Run export cases, measure results, and write the benchmark report."""
        root = self.config.run_root / "benchmarks" / uuid4().hex
        root.mkdir(parents=True)
        limits = detect_limits()
        source = PostgreSQLGraphSource(
            self.config.paths,
            GraphResourcePlanner(self.config.limits).plan(limits, DatabaseCapacity(1)).per_worker,
            self.config.postgres,
            LanguageSelector(load_selection_registry(self.config.registry_path)),
            IriPolicy(),
            snapshot_id=self.config.snapshot_id,
        )
        try:
            source.describe()
            capacity = source.database_capacity()
        finally:
            source.close()
        results: list[BenchmarkResult] = []
        for number, case in enumerate(cases):
            case_root = root / f"case-{number}"
            case_root.mkdir()
            self.case_roots.append(case_root)
            probe = ProcessTreeMemoryProbe()
            probe.start()
            started = time.perf_counter()
            effective_case: BenchmarkCase | None = None
            try:
                caps = replace(
                    self.config.limits,
                    threads=min(case.workers, self.config.limits.threads or case.workers),
                    batch_rows=min(case.batch_rows, self.config.limits.batch_rows),
                )
                config = replace(
                    self.config,
                    run_root=case_root,
                    limits=caps,
                    compression=case.compression,
                    compression_level=case.compression_level,
                )
                plan = GraphResourcePlanner(caps).plan(limits, capacity)
                effective_case = replace(case, workers=plan.workers, batch_rows=plan.per_worker.batches.max_rows)
                reader = PostgreSQLGraphSource(
                    config.paths,
                    plan.per_worker,
                    config.postgres,
                    LanguageSelector(load_selection_registry(config.registry_path)),
                    IriPolicy(),
                    snapshot_id=config.snapshot_id,
                )
                try:
                    identity = build_identity(config, reader)
                finally:
                    reader.close()
                jobs = (
                    RangeJob(
                        config,
                        identity,
                        QuestionRange(first, min(first + config.range_width, part.end_id)),
                        plan.per_worker,
                    )
                    for first in range(part.first_id, part.end_id, config.range_width)
                )
                markers = (
                    tuple(job() for job in jobs) if plan.workers == 1 else tuple(ParallelExporter().run(jobs, plan))
                )
                layout = ArtifactLayout(case_root)
                shards = tuple(record for marker in markers for record in iter_shards(layout, marker.shard_catalog))
                content = tuple(
                    sorted(record.part_key + ":" + record.graph_key + ":" + record.content_sha256 for record in shards)
                )
                stored = tuple(
                    sorted(record.part_key + ":" + record.graph_key + ":" + record.stored_sha256 for record in shards)
                )
                probe.stop()
                result = BenchmarkResult(
                    case,
                    sum(marker.question_count for marker in markers),
                    time.perf_counter() - started,
                    probe.peak_bytes,
                    sum(record.bytes for record in shards),
                    content,
                    True,
                    stored,
                    effective_case=effective_case,
                )
            except Exception as error:
                get_logger(__name__).exception("Graph benchmark case failed")
                probe.stop()
                result = BenchmarkResult(
                    case,
                    0,
                    time.perf_counter() - started,
                    probe.peak_bytes,
                    0,
                    (),
                    False,
                    (),
                    f"{type(error).__name__}: {error}",
                    effective_case=effective_case,
                )
            atomic_write_json(
                case_root / "benchmark.json", {"sample_only": True, "complete": False, "result": result.to_json()}
            )
            results.append(result)
        self.report_path = root / "report.json"
        atomic_write_json(
            self.report_path,
            {
                "sample_only": True,
                "complete": False,
                "cpu_limit": limits.cpus,
                "memory_available": limits.available_memory,
                "database_connection_slots": capacity.connection_slots,
                "results": [result.to_json() for result in results],
            },
        )
        return tuple(results)
