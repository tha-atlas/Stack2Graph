"""Select and validate exported graph artifacts without database credentials."""

import json
from hashlib import sha256
from pathlib import Path
from tempfile import mkdtemp

from graph.domain.artifacts import RESERVED_GRAPH_KEYS, ArtifactLayout
from graph.domain.index import IndexSelection, SelectedArtifacts
from graph.services.validate import artifact_shards, read_manifest
from utils.artifacts import AtomicBinaryWriter


def select_artifacts(layout: ArtifactLayout, selection: IndexSelection) -> SelectedArtifacts:
    """
    Stage a selected shard catalog; callers must remove its temporary root.

    Args:
        layout: Completed export whose shard metadata supplies index inputs.
        selection: Language graph keys; reserved metadata graphs join them.

    Returns:
        Selected inputs and their digest. The caller must remove the temporary
        directory containing shard_catalog after consuming it.

    Raises:
        ValueError: A graph is unavailable or a selected shard fails validation.

    """
    manifest = read_manifest(layout, require_complete=True)
    if any(key not in manifest.graph_keys or key in RESERVED_GRAPH_KEYS for key in selection.language_keys):
        raise ValueError("Selected language is absent from producer metadata")
    keys = tuple(sorted({*selection.language_keys, *RESERVED_GRAPH_KEYS}))
    root = Path(mkdtemp(prefix="kg-selected-"))
    catalog = AtomicBinaryWriter(root / "inputs.jsonl")
    digest = sha256(manifest.identity.digest.encode())
    digest.update(json.dumps(keys, separators=(",", ":")).encode())
    try:
        for record in artifact_shards(layout, manifest):
            if record.graph_key not in keys:
                continue
            path = layout.path(record.path)
            if path.is_symlink() or not path.is_file() or path.stat().st_size != record.bytes:
                raise ValueError(f"Selected shard metadata does not match a regular file: {record.path}")
            encoded = json.dumps(record.to_json(), sort_keys=True, separators=(",", ":")).encode()
            digest.update(encoded)
            row = {
                "path": str(path),
                "sha256": record.stored_sha256,
                "bytes": record.bytes,
                "compression": record.compression,
                "record_bytes": record.record_bytes,
            }
            catalog.stream.write((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode())
        catalog.publish()
    finally:
        catalog.discard()
    return SelectedArtifacts(manifest.identity, catalog.path, keys, digest.hexdigest(), layout.root)
