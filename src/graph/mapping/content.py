"""Map posts and comments using ontology types, properties, and inverses."""

from collections.abc import Iterator

from pyoxigraph import Literal, NamedNode, Triple

from graph.domain.source import CommentRecord, PostRecord, PostReference, VoteCounts, canonical_datetime
from graph.mapping.identity import NAMESPACE, IriPolicy
from graph.schema.contract import SchemaContract

RDF_TYPE = NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")


class ContentMapper:
    def __init__(self, contract: SchemaContract, iris: IriPolicy) -> None:
        """Bind the ontology contract and IRI policy used by content mappings."""
        self.contract, self.iris = contract, iris

    def types(self, subject: str, name: str) -> Iterator[Triple]:
        """Yield type triples for a class and each of its ontology ancestors."""
        for class_iri in self.contract.types(NAMESPACE + name):
            yield Triple(NamedNode(subject), RDF_TYPE, NamedNode(class_iri))

    def literal(self, subject: str, property_name: str, value: str | int | None) -> Iterator[Triple]:
        """Yield a typed literal triple, omitting None and normalizing dates."""
        if value is None:
            return
        prop = self.contract.properties[NAMESPACE + property_name]
        if not prop.datatype:
            raise ValueError("Literal requires an ontology datatype property")
        lexical = str(value)
        if prop.range.endswith("#dateTime"):
            lexical = canonical_datetime(lexical)
        yield Triple(
            NamedNode(subject), NamedNode(NAMESPACE + property_name), Literal(lexical, datatype=NamedNode(prop.range))
        )

    def relation(self, subject: str, name: str, target: str) -> Iterator[Triple]:
        """Yield an object-property edge and its declared inverse, if present."""
        prop = self.contract.properties[NAMESPACE + name]
        if prop.datatype:
            raise ValueError("Relation requires an ontology object property")
        yield Triple(NamedNode(subject), NamedNode(NAMESPACE + name), NamedNode(target))
        if prop.inverse is not None:
            yield Triple(NamedNode(target), NamedNode(prop.inverse), NamedNode(subject))

    def post(self, record: PostRecord, votes: VoteCounts, tag_name: str | None = None) -> Iterator[Triple]:
        """Yield supported post content and vote totals under the schema contract."""
        subject = self.iris.post(record, tag_name)
        name = {1: "Question", 2: "Answer", 4: "TagExcerpt", 5: "TagWiki"}.get(record.post_type)
        if name is None or min(votes.up, votes.down) < 0:
            raise ValueError("Unsupported source type or invalid vote counts")
        yield from self.types(subject, name)
        values: tuple[tuple[str, str | int | None], ...] = (
            ("sourcePostId", record.id),
            ("postTypeId", record.post_type),
            ("hasTitle", record.title),
            ("hasBody", record.body_html),
            ("score", record.score),
            ("commentCount", record.comment_count),
            ("upvoteCount", votes.up),
            ("downvoteCount", votes.down),
            ("creationDate", record.dates.creation),
            ("lastEditDate", record.dates.last_edit),
            ("lastActivityDate", record.dates.last_activity),
            ("communityOwnedDate", record.dates.community_owned),
            ("contentLicense", record.license),
        )
        for property_name, value in values:
            yield from self.literal(subject, property_name, value)
        if record.post_type == 1:
            for property_name, value in (
                ("answerCount", record.answer_count),
                ("viewCount", record.view_count),
                ("favoriteCount", record.favorite_count),
                ("closedDate", record.dates.closed),
            ):
                yield from self.literal(subject, property_name, value)

    def comment(self, record: CommentRecord, parent: PostReference, parent_iri: str) -> Iterator[Triple]:
        """Validate comment ownership and yield its content and parent relations."""
        if record.post_id != parent.id or parent.post_type not in (1, 2, 4, 5):
            raise ValueError("Comment parent is outside supported scope or inconsistent")
        subject = self.iris.comment(record.id, parent, parent_iri)
        name = "QuestionComment" if parent.post_type == 1 else "AnswerComment" if parent.post_type == 2 else "Comment"
        yield from self.types(subject, name)
        for property_name, value in (
            ("hasText", record.text),
            ("score", record.score),
            ("creationDate", record.creation),
            ("contentLicense", record.license),
        ):
            yield from self.literal(subject, property_name, value)
        yield from self.relation(parent_iri, "hasComment", subject)
