"""Read mapping constraints from the ontology as the vocabulary authority."""

from dataclasses import dataclass

from rdflib import OWL, RDF, RDFS, Graph, URIRef


@dataclass(frozen=True)
class PropertyContract:
    domain: str
    range: str
    datatype: bool
    inverse: str | None


@dataclass(frozen=True)
class SchemaContract:
    classes: frozenset[str]
    parents: dict[str, str]
    properties: dict[str, PropertyContract]
    disjoint: tuple[tuple[str, str], ...]

    @classmethod
    def from_ontology(cls, graph: Graph) -> SchemaContract:
        """Read ontology classes, property contracts, inverse edges, and disjointness."""
        classes = frozenset(str(subject) for subject in graph.subjects(RDF.type, OWL.Class))
        parents = {str(subject): str(parent) for subject, _, parent in graph.triples((None, RDFS.subClassOf, None))}
        properties: dict[str, PropertyContract] = {}
        for kind in (OWL.DatatypeProperty, OWL.ObjectProperty):
            for subject in graph.subjects(RDF.type, kind):
                domain = tuple(graph.objects(subject, RDFS.domain))
                ranges = tuple(graph.objects(subject, RDFS.range))
                if len(domain) != 1 or len(ranges) != 1 or not isinstance(subject, URIRef):
                    raise ValueError("Ontology property requires one domain/range and a named identity")
                inverse = tuple(graph.objects(subject, OWL.inverseOf))
                properties[str(subject)] = PropertyContract(
                    str(domain[0]), str(ranges[0]), kind == OWL.DatatypeProperty, str(inverse[0]) if inverse else None
                )
        for property_iri, contract in tuple(properties.items()):
            if contract.inverse is not None:
                partner = properties[contract.inverse]
                properties[contract.inverse] = PropertyContract(
                    partner.domain, partner.range, partner.datatype, property_iri
                )
        disjoint = tuple(
            sorted((str(subject), str(other)) for subject, _, other in graph.triples((None, OWL.disjointWith, None)))
        )
        return cls(classes, parents, properties, disjoint)

    def types(self, class_iri: str) -> tuple[str, ...]:
        """Return a class and its ancestors, rejecting unknown or cyclic classes."""
        result: list[str] = []
        while class_iri:
            if class_iri not in self.classes or class_iri in result:
                raise ValueError("Unknown or cyclic ontology class")
            result.append(class_iri)
            class_iri = self.parents.get(class_iri, "")
        return tuple(result)
