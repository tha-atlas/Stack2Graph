"""Define validation stage names and the systems each stage checks."""

XML_POSTGRES = "xml-postgres"
POSTGRES_QDRANT = "postgres-qdrant"
GRAPH_POSTGRES = "graph-postgres"
GRAPH_QDRANT = "graph-qdrant"

# A system fails only on comparisons attributed to it or on unattributed comparisons of a
# stage it owns. The pairing stage (graph-qdrant) has no owner, so its failures never
# rewrite either artifact's own validated state (docs/validation.md).
STAGE_OWNERS: dict[str, str] = {
    XML_POSTGRES: "postgres",
    POSTGRES_QDRANT: "vector",
    GRAPH_POSTGRES: "graph",
}
