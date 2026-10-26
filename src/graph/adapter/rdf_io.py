"""Stream validated RDF records without keeping an RDF database in memory."""

from collections.abc import Iterable, Iterator
from gzip import GzipFile
from typing import BinaryIO, Protocol, runtime_checkable

from pyoxigraph import Literal, NamedNode, Quad, RdfFormat, parse, serialize
from rdflib import Dataset, URIRef
from rdflib.exceptions import ParserError
from rdflib.plugins.parsers.nquads import NQuadsParser


@runtime_checkable
class ParsedLiteral(Protocol):
    """Define the RDFLib literal value access used without untyped operators."""

    @property
    def datatype(self) -> URIRef | None:
        """The literal's datatype IRI, when present."""
        ...

    @property
    def language(self) -> str | None:
        """The literal's language tag, when present."""
        ...

    def __str__(self) -> str:
        """Return the literal's lexical value."""
        ...


class RdfIO:
    def __init__(self, max_record_bytes: int) -> None:
        """Set a positive maximum byte budget for serialized RDF records."""
        if max_record_bytes < 1:
            raise ValueError("RDF record byte budget must be positive")
        self.max_record_bytes = max_record_bytes

    def record_bound(self, quad: Quad) -> int:
        """Validate quad node types and bound its escaped serialized size."""
        if (
            not isinstance(quad.subject, NamedNode)
            or not isinstance(quad.graph_name, NamedNode)
            or not isinstance(quad.object, NamedNode | Literal)
        ):
            raise ValueError("Instance RDF requires named subjects/graphs and no RDF-star or blank nodes")
        # Conservative escaping expansion; serialization itself remains native.
        size = 128 + sum(
            len(node.value.encode("utf-8")) * 6 for node in (quad.subject, quad.predicate, quad.graph_name)
        )
        value = quad.object
        size += len(value.value.encode("utf-8")) * 6
        if isinstance(value, Literal):
            size += len(value.datatype.value.encode("utf-8")) * 6
        if size > self.max_record_bytes:
            raise ValueError("Encoded RDF record exceeds configured byte budget")
        return size

    def _checked(self, quads: Iterable[Quad]) -> Iterator[Quad]:
        """Yield quads after checking their node types and byte budgets."""
        for quad in quads:
            self.record_bound(quad)
            yield quad

    def write(self, quads: Iterable[Quad], output: BinaryIO) -> None:
        """Serialize checked quads as N-Quads to an open binary stream."""
        serialize(self._checked(quads), output, RdfFormat.N_QUADS)

    def read(self, stream: BinaryIO | GzipFile) -> Iterator[Quad]:
        # Oxigraph caps its reader buffer at 16 MiB even for bytes input. Larger
        # bounded records use RDFLib with a dataset holding only that record.
        """Yield checked quads from bounded N-Quads lines."""
        while line := stream.readline(self.max_record_bytes + 1):
            if len(line) > self.max_record_bytes:
                raise ValueError("Encoded RDF line exceeds its declared record budget")
            if len(line) <= 16 * 1024**2:
                yield from self._checked(parse(line, RdfFormat.N_QUADS, lenient=False))
            else:
                dataset = Dataset()
                # The standard line parser avoids RDFLib's small-chunk reader,
                # which repeatedly copies a large single line.
                parser = NQuadsParser()
                parser.sink = dataset
                parser.line = line.decode("utf-8").rstrip("\r\n")
                try:
                    parser.parseline()
                except ParserError as error:
                    raise ValueError("Malformed large N-Quads statement") from error
                if len(dataset) != 1:
                    raise ValueError("Large N-Quads line must contain exactly one statement")
                for subject, predicate, obj, graph in dataset.quads((None, None, None, None)):
                    if (
                        not isinstance(subject, URIRef)
                        or not isinstance(predicate, URIRef)
                        or not isinstance(graph, URIRef)
                    ):
                        raise ValueError("Large RDF records require named subjects, predicates and graphs")
                    if isinstance(obj, URIRef):
                        native: NamedNode | Literal = NamedNode(str(obj))
                    elif isinstance(obj, ParsedLiteral):
                        native = (
                            Literal(str(obj), language=obj.language)
                            if obj.language
                            else Literal(str(obj), datatype=NamedNode(str(obj.datatype)) if obj.datatype else None)
                        )
                    else:
                        raise ValueError("Unsupported large RDF object")
                    yield from self._checked(
                        (Quad(NamedNode(str(subject)), NamedNode(str(predicate)), native, NamedNode(str(graph))),)
                    )
