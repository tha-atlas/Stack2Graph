"""Query a fixed graph scope through a supplied client and typed results."""

from graph import queries
from graph.adapter.sparql import SelectResult, SparqlTerm
from graph.domain.queries import AnswerComment, AnswerDetails, QuestionCounts, TagDocumentation
from graph.interfaces.queries import SelectClient
from graph.mapping.identity import IriPolicy

_INTEGER_TYPES = frozenset("http://www.w3.org/2001/XMLSchema#" + name for name in ("integer", "int", "long"))


def _binding(result: SelectResult, row: tuple[SparqlTerm | None, ...], variable: str) -> SparqlTerm | None:
    """Read a result binding, rejecting malformed row shape or variable names."""
    if len(row) != len(result.variables) or variable not in result.variables:
        raise ValueError(f"Malformed SPARQL binding: {variable}")
    return row[result.variables.index(variable)]


def _term(result: SelectResult, row: tuple[SparqlTerm | None, ...], variable: str) -> SparqlTerm:
    """Read a required bound SPARQL term."""
    term = _binding(result, row, variable)
    if term is None:
        raise ValueError(f"Unbound required SPARQL variable: {variable}")
    return term


def _literal(result: SelectResult, row: tuple[SparqlTerm | None, ...], variable: str) -> str:
    """Read a required literal binding and return its lexical value."""
    term = _term(result, row, variable)
    if term.kind != "literal":
        raise ValueError(f"Expected a literal for {variable}")
    return term.value


def _integer(result: SelectResult, row: tuple[SparqlTerm | None, ...], variable: str) -> int | None:
    """Read an optional typed integer binding, preserving unbound values."""
    term = _binding(result, row, variable)
    if term is None:
        return None
    if term.kind != "literal" or term.datatype not in _INTEGER_TYPES:
        raise ValueError(f"Expected an integer literal for {variable}")
    return int(term.value)


def _optional_literal(result: SelectResult, row: tuple[SparqlTerm | None, ...], variable: str) -> str | None:
    """Read a literal binding, or return None if it is unbound."""
    return None if _binding(result, row, variable) is None else _literal(result, row, variable)


def _single(result: SelectResult) -> tuple[SparqlTerm | None, ...]:
    """Require exactly one result row and return it."""
    if len(result.rows) != 1:
        raise ValueError("Expected exactly one query result")
    return result.rows[0]


class GraphQueries:
    """Query explicitly loaded graphs and deduplicate cross-graph results."""

    def __init__(self, client: SelectClient, graph_keys: tuple[str, ...], *, max_rows: int = 1000) -> None:
        """Validate loaded graph scope and the maximum query result size."""
        self.client = client
        self.graph_keys = tuple(sorted(set(graph_keys) - {"schema"}))
        if not self.graph_keys or not 1 <= max_rows <= 10000:
            raise ValueError("Queries require loaded data graphs and bounded result rows")
        for key in self.graph_keys:
            IriPolicy().graph(key)
        self.max_rows = max_rows

    def _graph(self, key: str) -> str:
        """Require a graph key to belong to the loaded scope."""
        if key not in self.graph_keys:
            raise ValueError(f"Graph {key!r} is not loaded")
        return key

    def linked_target_bodies(self, source_graph: str, source_id: int, target_id: int) -> tuple[str, ...]:
        """Read linked target bodies from the loaded target graphs."""
        result = self.client.select(
            queries.linked_target_bodies(
                self._graph(source_graph), self.graph_keys, source_id, target_id, self.max_rows
            )
        )
        return tuple(_literal(result, row, "body") for row in result.rows)

    def distinct_source_ids(self, ids: tuple[int, ...]) -> tuple[int, ...]:
        """Return sorted source IDs found in loaded graphs for a bounded ID batch."""
        if len(ids) > self.max_rows:
            raise ValueError("Question IDs exceed query batch limit")
        if not ids:
            return ()
        result = self.client.select(queries.distinct_source_ids(self.graph_keys, ids, self.max_rows))
        values = tuple(_integer(result, row, "id") for row in result.rows)
        if any(value is None for value in values):
            raise ValueError("Question source ID must be bound")
        return tuple(sorted(value for value in values if value is not None))

    def edge_present(self, graph: str, source_id: int, target_id: int, predicate: str) -> bool:
        """Return whether a supported question edge exists in a loaded graph."""
        return bool(self.client.select(queries.edge(self._graph(graph), source_id, target_id, predicate)).rows)

    def accepted_answer(self, graph: str, question_id: int) -> AnswerDetails:
        """Read one accepted answer with its optional body and score."""
        result = self.client.select(queries.accepted_answer(self._graph(graph), question_id))
        row = _single(result)
        answer = _term(result, row, "answer")
        if answer.kind != "uri":
            raise ValueError("Accepted answer must be an IRI")
        return AnswerDetails(answer.value, _optional_literal(result, row, "body"), _integer(result, row, "score"))

    def answer_comment(self, graph: str, question_id: int, answer_id: int, comment_id: int) -> AnswerComment:
        """Read one answer/comment pair with optional text and score fields."""
        result = self.client.select(queries.answer_comment(self._graph(graph), question_id, answer_id, comment_id))
        row = _single(result)
        return AnswerComment(
            _optional_literal(result, row, "body"),
            _optional_literal(result, row, "text"),
            _integer(result, row, "score"),
        )

    def tag_bundle(self, graph: str, tag_name: str) -> TagDocumentation:
        """Read tag identity and its optional wiki and excerpt bodies."""
        result = self.client.select(queries.tag_bundle(self._graph(graph), tag_name))
        row = _single(result)
        source_id = _integer(result, row, "id")
        if source_id is None:
            raise ValueError("Tag ID must be bound")
        return TagDocumentation(
            _literal(result, row, "name"),
            source_id,
            _optional_literal(result, row, "wikiBody"),
            _optional_literal(result, row, "excerptBody"),
        )

    def creation_date(self, graph: str, question_id: int) -> SparqlTerm | None:
        """Read an optional xsd:dateTime creation literal for a question."""
        result = self.client.select(queries.creation_date(self._graph(graph), question_id))
        term = _binding(result, _single(result), "created")
        if term is None:
            return None
        if term.kind != "literal" or term.datatype != "http://www.w3.org/2001/XMLSchema#dateTime":
            raise ValueError("Creation date must be an xsd:dateTime literal")
        return term

    def source_counts(self, graph: str, question_id: int) -> QuestionCounts:
        """Read optional question score, view, answer, and comment counts."""
        result = self.client.select(queries.source_counts(self._graph(graph), question_id))
        row = _single(result)
        return QuestionCounts(*(_integer(result, row, name) for name in ("score", "views", "answers", "comments")))
