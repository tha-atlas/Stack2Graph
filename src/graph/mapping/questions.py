"""Map one question group at a time into its assigned named graphs."""

from collections.abc import Iterable, Iterator

from pyoxigraph import NamedNode, Quad, Triple

from graph.domain.events import CommentEvent, LinkEvent, PostEvent, QuestionEnd, QuestionEvent, QuestionStart
from graph.domain.source import contradicts_observed_answers, resolved_accepted_answer
from graph.mapping.content import ContentMapper


def routed(triples: Iterable[Triple], graphs: tuple[str, ...], content: ContentMapper) -> Iterator[Quad]:
    """Copy each triple into every requested named graph."""
    for triple in triples:
        for graph in graphs:
            yield Quad(triple.subject, triple.predicate, triple.object, NamedNode(content.iris.graph(graph)))


class QuestionMapper:
    def __init__(self, content: ContentMapper) -> None:
        """Store the content mapper used for question event streams."""
        self.content = content

    def _start(self, start: QuestionStart) -> Iterator[Triple]:
        """Validate question state and yield content, answer status, and tag edges."""
        post = start.post
        if post.post_type != 1 or not start.graph_keys or len(set(start.graph_keys)) != len(start.graph_keys):
            raise ValueError("Invalid question start or graph membership")
        if "extra-languages" in start.graph_keys and len(start.graph_keys) > 1:
            raise ValueError("Extra-language membership must be exclusive")
        if contradicts_observed_answers(post.answer_count, start.observed_answers):
            raise ValueError("Contradictory source answer count")
        subject = self.content.iris.question(post.id)
        yield from self.content.post(post, start.votes)
        answered = (
            start.observed_answers > 0
            or (post.answer_count is not None and post.answer_count > 0)
            or start.accepted is not None
        )
        if answered:
            yield from self.content.types(subject, "AnsweredQuestion")
        elif post.answer_count == 0:
            yield from self.content.types(subject, "UnansweredQuestion")
        if post.accepted_answer_id is None:
            if start.accepted is not None:
                raise ValueError("Unexpected accepted-answer evidence")
            yield from self.content.types(subject, "QuestionWithoutAcceptedAnswer")
        else:
            accepted = start.accepted
            if accepted is None or accepted.id != post.accepted_answer_id:
                raise ValueError("Invalid accepted-answer reference")
            if resolved_accepted_answer(accepted, post.id):
                yield from self.content.types(subject, "QuestionWithAcceptedAnswer")
                yield from self.content.types(self.content.iris.answer(accepted.id), "AcceptedAnswer")
                yield from self.content.relation(subject, "acceptedAnswer", self.content.iris.answer(accepted.id))
        if set(post.tags) != {tag.name for tag in start.tags}:
            raise ValueError("Missing or inconsistent tag references")
        for tag in start.tags:
            yield from self.content.relation(subject, "taggedWith", self.content.iris.tag(tag.name))

    def _child(self, event: PostEvent | CommentEvent | LinkEvent, start: QuestionStart) -> Iterator[Triple]:
        """Validate child ownership and yield answer, comment, or link triples."""
        question = self.content.iris.question(start.post.id)
        if isinstance(event, PostEvent):
            if event.post.post_type != 2 or event.post.parent_id != start.post.id:
                raise ValueError("Answer must belong to the active question")
            yield from self.content.post(event.post, event.votes)
            yield from self.content.relation(question, "hasAnswer", self.content.iris.answer(event.post.id))
        elif isinstance(event, CommentEvent):
            parent = event.parent
            if parent.post_type == 1 and parent.id == start.post.id:
                parent_iri = question
            elif parent.post_type == 2 and parent.parent_id == start.post.id:
                parent_iri = self.content.iris.answer(parent.id)
            else:
                raise ValueError("Comment does not belong to the active question")
            yield from self.content.comment(event.comment, parent, parent_iri)
        else:
            if (
                event.source_id != start.post.id
                or not event.target.exists
                or event.target.post_type != 1
                or event.link_type not in (1, 3)
                or event.source_rows < 1
            ):
                raise ValueError("Invalid outgoing question relation")
            yield from self.content.relation(
                question,
                "linkedTo" if event.link_type == 1 else "duplicateOf",
                self.content.iris.question(event.target.id),
            )

    def map(self, events: Iterable[QuestionEvent]) -> Iterator[Quad]:
        """Map a balanced question event stream to graph-scoped quads."""
        active: QuestionStart | None = None
        for event in events:
            if isinstance(event, QuestionStart):
                if active is not None:
                    raise ValueError("Question started before previous question ended")
                active = event
                yield from routed(self._start(event), event.graph_keys, self.content)
            elif isinstance(event, QuestionEnd):
                if active is None or event.question_id != active.post.id:
                    raise ValueError("Question end without matching start")
                active = None
            else:
                if active is None:
                    raise ValueError("Child event without active question")
                yield from routed(self._child(event, active), active.graph_keys, self.content)
        if active is not None:
            raise ValueError("Incomplete question event stream")
