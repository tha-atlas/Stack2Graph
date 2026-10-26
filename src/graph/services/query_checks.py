"""Compare recorded export counts with logical source counts in QLever."""

from dataclasses import replace
from pathlib import Path

from graph.adapter.sparql import SparqlClient
from graph.domain.artifacts import ArtifactLayout, ArtifactManifest
from graph.domain.index import IndexManifest, QLeverSettings, QueryAcceptance, SelectedArtifacts
from graph.mapping.identity import NAMESPACE
from utils.artifacts import atomic_write_json
from utils.types import json_integer, json_object, parse_json

COUNT_QUERIES: tuple[tuple[str, str], ...] = (
    (
        "Posts",
        f"SELECT (COUNT(DISTINCT ?post) AS ?count) WHERE {{ GRAPH ?g {{ ?post <{NAMESPACE}sourcePostId> ?id }} }}",
    ),
    (
        "Comments",
        f"SELECT (COUNT(DISTINCT ?comment) AS ?count) WHERE {{ GRAPH ?g {{ ?comment <{NAMESPACE}commentOn> ?post }} }}",
    ),
    (
        "Tags",
        f"SELECT (COUNT(DISTINCT ?tag) AS ?count) WHERE {{ GRAPH ?g {{ ?tag <{NAMESPACE}tagId> ?id }} }}",
    ),
    (
        "Votes",
        f"""SELECT ((SUM(?up) + SUM(?down)) AS ?count) WHERE {{
 {{ SELECT ?post (MAX(?upValue) AS ?up) (MAX(?downValue) AS ?down) WHERE {{
  GRAPH ?g {{ ?post <{NAMESPACE}upvoteCount> ?upValue ; <{NAMESPACE}downvoteCount> ?downValue }}
 }} GROUP BY ?post }}
}}""",
    ),
    (
        "PostLinks",
        f"""SELECT (COUNT(*) AS ?count) WHERE {{
 {{ SELECT DISTINCT ?source ?predicate ?target WHERE {{
  GRAPH ?g {{ ?source ?predicate ?target }}
  VALUES ?predicate {{ <{NAMESPACE}linkedTo> <{NAMESPACE}duplicateOf> }}
 }} }}
}}""",
    ),
)


def write_expectations(
    inputs: SelectedArtifacts, layout: ArtifactLayout, manifest: ArtifactManifest, candidate: Path
) -> None:
    """Persist five expected counts without rescanning source or shard data."""
    from graph.services.validate import artifact_shards

    counts = dict(manifest.processed_table_counts)
    counts["PostLinks"] = sum(marker.distinct_links for marker in manifest.iter_ranges(layout))
    selected = set(inputs.graph_keys)
    complete_selection = all(record.graph_key in selected for record in artifact_shards(layout, manifest))
    atomic_write_json(
        candidate / "expected-queries.json",
        {"complete_selection": complete_selection, "counts": counts},
    )


def _single(client: SparqlClient, query: str) -> str:
    """Require one bound scalar value from an acceptance query."""
    result = client.select(query)
    if len(result.rows) != 1 or len(result.rows[0]) != 1 or result.rows[0][0] is None:
        raise ValueError("Acceptance query did not return exactly one bound value")
    return result.rows[0][0].value


def verify_queries(client: SparqlClient, candidate: Path) -> QueryAcceptance:
    """Check query expectations and persist matching acceptance and index flags."""
    index = IndexManifest.from_json(json_object(parse_json((candidate / "index.json").read_text())))
    expected = json_object(parse_json((candidate / "expected-queries.json").read_text()))
    complete_selection = expected["complete_selection"]
    if not isinstance(complete_selection, bool):
        raise ValueError("Acceptance selection marker must be boolean")
    if not complete_selection:
        if not client.ask("ASK { GRAPH ?g { ?subject ?predicate ?object } }"):
            raise ValueError("Partial QLever selection is empty")
        acceptance = QueryAcceptance(index.digest, True, ("partial-selection-queryable",))
        atomic_write_json(candidate / "acceptance.json", acceptance.to_json())
        atomic_write_json(candidate / "index.json", replace(index, verified=True).to_json())
        return acceptance
    counts = json_object(expected["counts"])
    checked: list[str] = []
    for table, query in COUNT_QUERIES:
        actual = int(_single(client, query))
        wanted = json_integer(counts[table])
        if actual != wanted:
            raise ValueError(f"QLever {table} count differs from exported rows: expected {wanted}, got {actual}")
        checked.append(table)
    acceptance = QueryAcceptance(index.digest, True, tuple(checked))
    atomic_write_json(candidate / "acceptance.json", acceptance.to_json())
    atomic_write_json(candidate / "index.json", replace(index, verified=True).to_json())
    return acceptance


def verify_candidate(settings: QLeverSettings, candidate: Path, *, instance_key: str | None = None) -> QueryAcceptance:
    """Validate on an independently owned temporary loopback endpoint."""
    import socket
    from uuid import uuid4

    from graph.adapter.qlever import QLeverAdapter

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        address: tuple[str, int] = listener.getsockname()
    temporary = replace(
        settings, host="127.0.0.1", port=address[1], instance_key=instance_key or "verify-" + uuid4().hex
    )
    adapter = QLeverAdapter(temporary)
    if instance_key is not None:
        adapter.stop(candidate)
    endpoint = adapter.start(candidate)
    try:
        return verify_queries(SparqlClient(endpoint, temporary.timeout_seconds), candidate)
    finally:
        adapter.stop(candidate)
