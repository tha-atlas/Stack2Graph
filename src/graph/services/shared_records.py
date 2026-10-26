"""Export shared records in a separately checkpointed source-mapping pass."""

from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from graph.adapter.shards import GraphShardWriter
from graph.domain.artifacts import ShardCatalogRef, require_processed_table_counts
from graph.domain.events import CommentEvent, PostEvent, SharedEvent, SharedStart
from graph.domain.source import IMPORTED_TABLES, TagUsage
from graph.interfaces.source import GraphSource
from graph.mapping.shared import SharedMapper
from utils.resources import BatchLimits


@dataclass(frozen=True)
class SharedExport:
    catalog: ShardCatalogRef
    processed_table_counts: tuple[tuple[str, int], ...]


class SharedRecordExporter:
    def __init__(self, source: GraphSource, mapper: SharedMapper) -> None:
        """Bind the shared source and its graph mapper."""
        self.source, self.mapper = source, mapper

    def run(self, usages: Iterable[TagUsage], writer: GraphShardWriter, limits: BatchLimits) -> SharedExport:
        """Publish shared content and return its catalog and source-row counts."""
        counts: dict[str, int] = dict.fromkeys(IMPORTED_TABLES, 0)

        def counted() -> Iterator[SharedEvent]:
            # The source emits each tag and each documentation/orphan record exactly once,
            # so every event contributes its source row to exactly one table counter.
            """Count source rows once while forwarding shared events."""
            for event in self.source.iter_shared(usages, limits):
                if isinstance(event, SharedStart):
                    if event.tag is not None:
                        counts["Tags"] += 1
                elif isinstance(event, PostEvent):
                    counts["Posts"] += 1
                    counts["Votes"] += event.votes.up + event.votes.down
                elif isinstance(event, CommentEvent):
                    counts["Comments"] += 1
                yield event

        writer.write(self.mapper.map(counted()))
        return SharedExport(writer.finish(), require_processed_table_counts(tuple(counts.items())))
