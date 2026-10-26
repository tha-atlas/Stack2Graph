"""Budget CPUs, available memory, and disk space with recorded limit evidence."""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from utils.types import JSONObject

MIB = 1024**2


@dataclass(frozen=True)
class BatchLimits:
    """Limit the rows and bytes held in one processing batch."""

    max_rows: int = 65536
    max_bytes: int = 32 * 1024 * 1024
    max_record_bytes: int = 16 * 1024 * 1024

    def __post_init__(self) -> None:
        """Require positive limits and a record budget within the batch budget."""
        if min(self.max_rows, self.max_bytes, self.max_record_bytes) <= 0:
            raise ValueError("Batch limits must be positive")
        if self.max_record_bytes > self.max_bytes:
            raise ValueError("Record budget cannot exceed batch budget")


@dataclass(frozen=True)
class ResourceCaps:
    threads: int | None = None
    memory_bytes: int | None = None
    batch_rows: int = 65536
    batch_bytes: int | None = None
    record_bytes: int | None = None
    spill_bytes: int = 8 * 1024**3
    disk_reserve: int = 256 * MIB


@dataclass(frozen=True)
class ResourceLimits:
    cpus: int
    available_memory: int
    evidence: tuple[str, ...]


@dataclass(frozen=True)
class ResourceBudget:
    """Record usable resources after safety reserves and configured caps."""

    threads: int
    memory_bytes: int
    evidence: tuple[str, ...]

    def __post_init__(self) -> None:
        """Require positive CPU and memory allowances."""
        if min(self.threads, self.memory_bytes) < 1:
            raise ValueError("Resource budgets require positive CPU and RAM allowances")


def resource_budget(
    limits: ResourceLimits, *, threads: int | None = None, memory_bytes: int | None = None
) -> ResourceBudget:
    """Apply the shared startup policy once: effective CPUs and 75% available RAM."""
    if limits.cpus < 1 or limits.available_memory < 0:
        raise ValueError("Detected resource limits are invalid")
    available = limits.available_memory * 3 // 4
    if available < 1:
        raise RuntimeError("No remaining RAM for a resource budget")
    return cap_budget(
        ResourceBudget(limits.cpus, available, limits.evidence), threads=threads, memory_bytes=memory_bytes
    )


def cap_budget(
    budget: ResourceBudget, *, threads: int | None = None, memory_bytes: int | None = None
) -> ResourceBudget:
    """Apply optional limits to an allowance without taking another safety reserve."""
    if any(value is not None and value < 1 for value in (threads, memory_bytes)):
        raise ValueError("Resource caps must be positive")
    return ResourceBudget(
        min(budget.threads, threads or budget.threads),
        min(budget.memory_bytes, memory_bytes or budget.memory_bytes),
        budget.evidence,
    )


def split_budget(budget: ResourceBudget, workers: int) -> ResourceBudget:
    """Divide an existing allowance without applying host safety reserves again."""
    if not 1 <= workers <= min(budget.threads, budget.memory_bytes):
        raise ValueError("Budget partitions require one CPU and positive RAM per worker")
    return ResourceBudget(budget.threads // workers, budget.memory_bytes // workers, budget.evidence)


def worker_capacity(
    budget: ResourceBudget,
    memory_per_worker: int,
    *,
    threads_per_worker: int = 1,
    reserved_threads: int = 0,
    reserved_memory_bytes: int = 0,
    max_workers: int | None = None,
) -> int:
    """Bound workers by CPU, RAM, explicit capacity and producer/writer headroom."""
    if min(memory_per_worker, threads_per_worker) < 1 or min(reserved_threads, reserved_memory_bytes) < 0:
        raise ValueError("Worker costs must be positive and reserves nonnegative")
    if max_workers is not None and max_workers < 0:
        raise ValueError("Worker capacity cannot be negative")
    return min(
        max(0, budget.threads - reserved_threads) // threads_per_worker,
        max(0, budget.memory_bytes - reserved_memory_bytes) // memory_per_worker,
        max_workers if max_workers is not None else budget.threads,
    )


@dataclass(frozen=True)
class ResourcePlan:
    threads: int
    memory_bytes: int
    database_bytes: int
    batches: BatchLimits
    spill_bytes: int
    disk_reserve: int
    evidence: tuple[str, ...]

    def to_json(self) -> JSONObject:
        """Return the resource plan as JSON-compatible fields."""
        return {
            "threads": self.threads,
            "memory_bytes": self.memory_bytes,
            "database_bytes": self.database_bytes,
            "batches": {
                "max_rows": self.batches.max_rows,
                "max_bytes": self.batches.max_bytes,
                "max_record_bytes": self.batches.max_record_bytes,
            },
            "spill_bytes": self.spill_bytes,
            "disk_reserve": self.disk_reserve,
            "evidence": list(self.evidence),
        }


def _number(path: Path) -> int | None:
    """Read a bounded nonnegative integer, or return None on failure."""
    try:
        value = int(path.read_text().strip())
        return value if 0 <= value < 1 << 60 else None
    except OSError, ValueError:
        return None


def cgroup_limits(membership: str, mountinfo: str) -> tuple[int | None, int | None, tuple[str, ...]]:
    """Read ancestor cgroup CPU and remaining memory limits with evidence."""
    cpu_limits, memory_limits, evidence = [], [], []
    for member in membership.splitlines():
        _, controllers, group = member.split(":", 2)
        for mount in mountinfo.splitlines():
            left, right = mount.split(" - ", 1)
            fields, options = left.split(), right.split()
            version2 = options[0] == "cgroup2" and not controllers
            version1 = options[0] == "cgroup" and bool(set(controllers.split(",")) & set(options[-1].split(",")))
            if not (version2 or version1):
                continue
            base, root = Path(fields[4].replace("\\040", " ")), Path(fields[3])
            group_path = Path(group)
            # A cgroup namespace may expose membership relative to its mount root.
            relative = (
                group_path.relative_to(root) if group_path.is_relative_to(root) else Path(str(group_path).lstrip("/"))
            )
            current = base / relative
            while current.is_relative_to(base):
                limit_name, used_name = (
                    ("memory.max", "memory.current") if version2 else ("memory.limit_in_bytes", "memory.usage_in_bytes")
                )
                limit, used = _number(current / limit_name), _number(current / used_name)
                if limit is not None:
                    remaining = max(0, limit - (used or 0))
                    memory_limits.append(remaining)
                    evidence.append(f"{current / limit_name}: remaining={remaining}")
                try:
                    if version2:
                        quota, period = (current / "cpu.max").read_text().split()
                        if quota != "max":
                            cpu_limits.append(max(1, int(quota) // int(period)))
                    else:
                        quota_number, period_number = (
                            _number(current / "cpu.cfs_quota_us"),
                            _number(current / "cpu.cfs_period_us"),
                        )
                        if quota_number is not None and period_number:
                            cpu_limits.append(max(1, quota_number // period_number))
                except OSError, ValueError:
                    pass
                if current == base:
                    break
                current = current.parent
    return min(cpu_limits, default=None), min(memory_limits, default=None), tuple(evidence)


def _available_memory() -> int:
    """Read available RAM through a runtime import of psutil."""
    try:
        import psutil
    except ImportError:
        try:
            return next(
                int(line.split()[1]) * 1024
                for line in Path("/proc/meminfo").read_text().splitlines()
                if line.startswith("MemAvailable:")
            )
        except OSError, StopIteration, ValueError:
            return 512 * MIB
    return psutil.virtual_memory().available


def detect_limits() -> ResourceLimits:
    """Detect effective CPU and available RAM limits, including cgroups."""
    cpus = os.process_cpu_count() or 1
    available = _available_memory()
    evidence = [f"affinity CPUs={cpus}"]
    try:
        cpu, memory, details = cgroup_limits(
            Path("/proc/self/cgroup").read_text(),
            Path("/proc/self/mountinfo").read_text(),
        )
        cpus = min(cpus, cpu) if cpu else cpus
        available = min(available, memory) if memory is not None else available
        evidence.extend(details)
    except OSError:
        evidence.append("Linux limit files unavailable; conservative fallback")
    evidence.append(f"effective available bytes={available}")
    return ResourceLimits(cpus, available, tuple(evidence))


def _validate_caps(caps: ResourceCaps) -> None:
    """Reject nonpositive resource caps."""
    numbers = [caps.batch_rows, caps.spill_bytes, caps.disk_reserve]
    numbers.extend(x for x in (caps.threads, caps.memory_bytes, caps.batch_bytes, caps.record_bytes) if x is not None)
    if min(numbers) <= 0:
        raise ValueError("Resource caps must be positive")


def plan_resources(limits: ResourceLimits, caps: ResourceCaps, *, scalable: bool = False) -> ResourcePlan:
    """Build a resource plan from detected limits and configured caps."""
    _validate_caps(caps)
    startup = resource_budget(
        limits,
        threads=caps.threads if caps.threads is not None else (None if scalable else 4),
        memory_bytes=caps.memory_bytes if caps.memory_bytes is not None else (None if scalable else 8 * 1024**3),
    )
    return plan_from_budget(startup, caps, scalable=scalable)


def plan_from_budget(budget: ResourceBudget, caps: ResourceCaps, *, scalable: bool = False) -> ResourcePlan:
    """Size batches and database memory within an already assigned allowance."""
    _validate_caps(caps)
    effective = cap_budget(budget, threads=caps.threads, memory_bytes=caps.memory_bytes)
    memory, threads = effective.memory_bytes, effective.threads
    if memory < 128 * MIB:
        raise RuntimeError(f"Insufficient assigned RAM: {memory} bytes")
    automatic_batch = min(256 * MIB, max(32 * MIB, memory // max(16, threads * 16))) if scalable else 32 * MIB
    batch = min(caps.batch_bytes or automatic_batch, memory // 16)
    record = min(caps.record_bytes or 16 * MIB, batch)
    if caps.record_bytes and caps.record_bytes > batch:
        raise ValueError("Explicit record budget exceeds effective batch budget")
    return ResourcePlan(
        threads,
        memory,
        memory // 2,
        BatchLimits(caps.batch_rows, batch, record),
        caps.spill_bytes,
        caps.disk_reserve,
        budget.evidence,
    )


class DiskBudget:
    """Track temporary-file bytes and reserve free disk space for a stage."""

    def __init__(self, disk_reserve: int, spill_limit: int) -> None:
        """Set disk reserves and initialize temporary-file byte accounting."""
        if disk_reserve < 0 or spill_limit < 1:
            raise ValueError("Disk budgets require nonnegative reserve and positive spill limit")
        self.disk_reserve, self.spill_limit = disk_reserve, spill_limit
        self.spill_used = 0

    @classmethod
    def from_resources(cls, resources: ResourcePlan | None = None) -> Self:
        """Build a disk budget from a resource plan or the default caps."""
        caps = ResourceCaps()
        return cls(
            resources.disk_reserve if resources else caps.disk_reserve,
            resources.spill_bytes if resources else caps.spill_bytes,
        )

    def require(self, directory: Path, size: int, *, spill: bool = False) -> None:
        """Check free disk space and charge spill bytes when requested."""
        if size < 0:
            raise ValueError("Disk byte requests cannot be negative")
        require_disk(directory, size, self.disk_reserve)
        if spill:
            if self.spill_used + size > self.spill_limit:
                raise RuntimeError("Temporary files exceed configured spill byte budget")
            self.spill_used += size

    def release_file(self, path: Path) -> None:
        """Subtract a file's size from spill accounting without deleting it."""
        size = path.stat().st_size
        if size > self.spill_used:
            raise RuntimeError("Spill accounting underflow")
        self.spill_used -= size


def require_disk(path: Path, needed: int, reserve: int = 0) -> None:
    """Require enough free space for the requested bytes and reserve."""
    while not path.exists():
        path = path.parent
    free = shutil.disk_usage(path).free
    if free < needed + reserve:
        raise RuntimeError(f"Insufficient disk at {path}: need {needed + reserve}, free {free} bytes")


def require_writable_directory(path: Path) -> None:
    """Probe actual writes without leaving a file or following directory symlinks."""
    if not path.is_absolute() or path.resolve() != path:
        raise ValueError(f"Data directory must be absolute and must not follow a symlink: {path}")
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryFile(dir=path) as stream:
            stream.write(b"preflight")
            stream.flush()
            os.fsync(stream.fileno())
    except OSError as error:
        raise RuntimeError(f"Data directory is not writable: {path}: {error}") from error


def require_memory(needed: int, limits: ResourceLimits | None = None) -> None:
    """Stop before new buffers when effective remaining RAM becomes insufficient."""
    current = limits or detect_limits()
    if current.available_memory < max(64 * MIB, needed):
        raise RuntimeError(
            f"Insufficient remaining RAM: need {max(64 * MIB, needed)}, available {current.available_memory} bytes",
        )
