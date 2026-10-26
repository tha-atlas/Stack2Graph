"""Divide CPU, RAM, and database capacity among independent range workers."""

from dataclasses import dataclass, replace

from graph.domain.source import DatabaseCapacity
from utils.resources import (
    MIB,
    ResourceCaps,
    ResourceLimits,
    ResourcePlan,
    plan_from_budget,
    resource_budget,
    split_budget,
    worker_capacity,
)


@dataclass(frozen=True)
class GraphResourcePlan:
    workers: int
    pending_jobs: int
    per_worker: ResourcePlan
    coordinator: ResourcePlan
    total_memory_bytes: int
    connection_budget: int


class GraphResourcePlanner:
    def __init__(self, caps: ResourceCaps) -> None:
        """Store the resource caps used to size graph export work."""
        self.caps = caps

    def plan(self, limits: ResourceLimits, capacity: DatabaseCapacity) -> GraphResourcePlan:
        """Size workers and per-worker budgets within CPU, RAM, and DB capacity."""
        budget = resource_budget(limits, threads=self.caps.threads, memory_bytes=self.caps.memory_bytes)
        workers = worker_capacity(budget, 256 * MIB, max_workers=capacity.connection_slots)
        if workers < 1:
            raise ValueError("Graph budget cannot support one 256 MiB worker and database connection")
        coordinator = plan_from_budget(budget, replace(self.caps, threads=1), scalable=True)
        share = replace(split_budget(budget, workers), threads=1)
        # Divide temporary-space allowance and leave room for concurrent record writes.
        encoded_record = (self.caps.record_bytes or 16 * MIB) * 6 + 65536
        caps = replace(
            self.caps,
            threads=1,
            memory_bytes=share.memory_bytes,
            spill_bytes=max(1, self.caps.spill_bytes // workers),
            disk_reserve=self.caps.disk_reserve + workers * encoded_record,
        )
        per_worker = plan_from_budget(share, caps, scalable=True)
        return GraphResourcePlan(workers, workers * 2, per_worker, coordinator, budget.memory_bytes, workers)
