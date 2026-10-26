"""Send bounded SPARQL HTTP requests and decode typed JSON result bindings."""

from dataclasses import dataclass
from http.client import HTTPResponse
from typing import Literal, Protocol
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from utils.types import JSONObject, json_array, json_object, json_string, parse_json


class HTTPTransport(Protocol):
    def __call__(self, request: Request, /, *, timeout: int) -> HTTPResponse:
        """Open an HTTP response for a request with an explicit timeout."""
        ...


OPEN_HTTP: HTTPTransport = urlopen


@dataclass(frozen=True)
class SparqlTerm:
    kind: Literal["uri", "literal", "bnode"]
    value: str
    datatype: str | None
    language: str | None


@dataclass(frozen=True)
class SelectResult:
    variables: tuple[str, ...]
    rows: tuple[tuple[SparqlTerm | None, ...], ...]


class SparqlClient:
    def __init__(self, endpoint: str, timeout_seconds: int) -> None:
        """Validate and store the SPARQL endpoint and request timeout."""
        if not endpoint.startswith(("http://", "https://")) or timeout_seconds < 1:
            raise ValueError("Invalid SPARQL endpoint or timeout")
        self.endpoint, self.timeout_seconds = endpoint, timeout_seconds

    def _request(self, query: str) -> JSONObject:
        """Post a SPARQL query and decode a bounded JSON response."""
        request = Request(
            self.endpoint,
            data=urlencode({"query": query}).encode(),
            headers={"Accept": "application/sparql-results+json", "Content-Type": "application/x-www-form-urlencoded"},
        )
        with OPEN_HTTP(request, timeout=self.timeout_seconds) as response:
            content = response.read(16 * 1024**2 + 1)
        if len(content) > 16 * 1024**2:
            raise ValueError("SPARQL result exceeds response budget; use bounded queries")
        return json_object(parse_json(content.decode("utf-8")))

    @staticmethod
    def _term(value: JSONObject) -> SparqlTerm:
        """Decode a URI, literal, or blank-node SPARQL result term."""
        kind = json_string(value["type"])
        if kind not in ("uri", "literal", "bnode"):
            raise ValueError("Unknown SPARQL term kind")
        term_kind: Literal["uri", "literal", "bnode"] = (
            "uri" if kind == "uri" else "bnode" if kind == "bnode" else "literal"
        )
        datatype, language = value.get("datatype"), value.get("xml:lang")
        return SparqlTerm(
            term_kind,
            json_string(value["value"]),
            None if datatype is None else json_string(datatype),
            None if language is None else json_string(language),
        )

    def select(self, query: str) -> SelectResult:
        """Execute SELECT and retain variable order and unbound result cells."""
        value = self._request(query)
        variables = tuple(json_string(variable) for variable in json_array(json_object(value["head"])["vars"]))
        rows: list[tuple[SparqlTerm | None, ...]] = []
        for binding in json_array(json_object(value["results"])["bindings"]):
            fields = json_object(binding)
            rows.append(
                tuple(
                    None if variable not in fields else self._term(json_object(fields[variable]))
                    for variable in variables
                )
            )
        return SelectResult(variables, tuple(rows))

    def ask(self, query: str) -> bool:
        """Execute ASK and require a boolean result."""
        value = self._request(query)["boolean"]
        if not isinstance(value, bool):
            raise ValueError("SPARQL ASK result must be boolean")
        return value
