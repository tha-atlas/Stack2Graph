"""Precise supported Pyoxigraph 0.5.11 APIs; bundled parser iteration uses Any."""

from collections.abc import Iterable, Iterator
from gzip import GzipFile
from os import PathLike
from typing import BinaryIO, ClassVar

class NamedNode:
    def __init__(self, value: str, /) -> None: ...
    @property
    def value(self) -> str: ...
    def __str__(self) -> str: ...

class BlankNode:
    def __init__(self, value: str | None = None, /) -> None: ...

class DefaultGraph: ...

class Literal:
    def __init__(
        self, value: str | int | float | bool, /, *, datatype: NamedNode | None = None, language: str | None = None
    ) -> None: ...
    @property
    def value(self) -> str: ...
    @property
    def datatype(self) -> NamedNode: ...
    @property
    def language(self) -> str | None: ...
    def __str__(self) -> str: ...

class Triple:
    def __init__(
        self,
        subject: NamedNode | BlankNode | Triple,
        predicate: NamedNode,
        obj: NamedNode | BlankNode | Literal | Triple,
        /,
    ) -> None: ...
    @property
    def subject(self) -> NamedNode | BlankNode | Triple: ...
    @property
    def predicate(self) -> NamedNode: ...
    @property
    def object(self) -> NamedNode | BlankNode | Literal | Triple: ...
    def __str__(self) -> str: ...

class Quad:
    def __init__(
        self,
        subject: NamedNode | BlankNode | Triple,
        predicate: NamedNode,
        obj: NamedNode | BlankNode | Literal | Triple,
        graph_name: NamedNode | BlankNode | DefaultGraph | None = None,
        /,
    ) -> None: ...
    @property
    def subject(self) -> NamedNode | BlankNode | Triple: ...
    @property
    def predicate(self) -> NamedNode: ...
    @property
    def object(self) -> NamedNode | BlankNode | Literal | Triple: ...
    @property
    def graph_name(self) -> NamedNode | BlankNode | DefaultGraph: ...
    def __str__(self) -> str: ...

class RdfFormat:
    N_QUADS: ClassVar[RdfFormat]
    N_TRIPLES: ClassVar[RdfFormat]

class QuadParser(Iterator[Quad]):
    def __iter__(self) -> QuadParser: ...
    def __next__(self) -> Quad: ...

def parse(
    data: bytes | str | BinaryIO | GzipFile | None = None,
    rdf_format: RdfFormat | None = None,
    /,
    *,
    path: str | PathLike[str] | None = None,
    base_iri: str | None = None,
    without_named_graphs: bool = False,
    rename_blank_nodes: bool = False,
    lenient: bool = False,
) -> QuadParser: ...
def serialize(
    data: Iterable[Triple] | Iterable[Quad],
    output: BinaryIO | str | PathLike[str] | None = None,
    rdf_format: RdfFormat | None = None,
    /,
    *,
    prefixes: dict[str, str] | None = None,
    base_iri: str | None = None,
) -> bytes | None: ...
