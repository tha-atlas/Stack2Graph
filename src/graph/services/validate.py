"""Read completed graph manifests and the shard catalogs they reference."""

from collections.abc import Iterator

from graph.adapter.inventory import iter_shards, verify_file
from graph.domain.artifacts import (
    RESERVED_GRAPH_KEYS,
    ArtifactLayout,
    ArtifactManifest,
    FileRef,
    ShardCatalogRef,
    ShardRecord,
)
from utils.types import JSONObject, json_object, parse_json


def read_manifest(layout: ArtifactLayout, *, require_complete: bool = False) -> ArtifactManifest:
    """Read bounded manifest metadata and validate provenance and graph scope."""
    if layout.manifest.stat().st_size > 16 * 1024**2:
        raise ValueError("Manifest exceeds metadata budget")
    manifest = ArtifactManifest.from_json(json_object(parse_json(layout.manifest.read_text())))
    if require_complete and not manifest.complete:
        raise ValueError("Indexing requires a complete producer manifest")
    if (
        manifest.inventory.snapshot.digest != manifest.identity.snapshot_digest
        or manifest.inventory.snapshot.item_id != manifest.identity.snapshot_id
    ):
        raise ValueError("Manifest snapshot provenance is inconsistent")
    if len(set(manifest.graph_keys)) != len(manifest.graph_keys) or not RESERVED_GRAPH_KEYS.issubset(
        manifest.graph_keys
    ):
        raise ValueError("Manifest graph dependencies are invalid")
    return manifest


def marker_document(layout: ArtifactLayout, reference: FileRef) -> JSONObject:
    """Verify a marker file and decode its JSON object."""
    return json_object(parse_json(verify_file(layout, reference).read_text()))


def artifact_catalogs(layout: ArtifactLayout, manifest: ArtifactManifest) -> Iterator[ShardCatalogRef]:
    """Yield range, shared-content, and schema shard catalog references."""
    for marker in manifest.iter_ranges(layout):
        yield marker.shard_catalog
    for reference in (manifest.shared_marker, manifest.schema_marker):
        yield ShardCatalogRef.from_json(json_object(marker_document(layout, reference)["catalog"]))


def artifact_shards(layout: ArtifactLayout, manifest: ArtifactManifest) -> Iterator[ShardRecord]:
    """Yield verified catalog records for all exported artifact shards."""
    for catalog in artifact_catalogs(layout, manifest):
        yield from iter_shards(layout, catalog)
