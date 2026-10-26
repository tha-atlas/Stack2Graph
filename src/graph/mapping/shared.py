"""Map shared tags and documentation with common content and inverse rules."""

from collections.abc import Iterable, Iterator

from pyoxigraph import Quad, Triple

from graph.domain.events import CommentEvent, PostEvent, SharedEnd, SharedEvent, SharedStart
from graph.domain.source import PostRecord, TagRecord
from graph.mapping.content import ContentMapper
from graph.mapping.questions import routed


class SharedMapper:
    def __init__(self, content: ContentMapper) -> None:
        """Store the content mapper used for shared tag and documentation records."""
        self.content = content

    def _tag(self, tag: TagRecord) -> Iterator[Triple]:
        """Yield a tag's type, source ID, name, and declared question count."""
        subject = self.content.iris.tag(tag.name)
        yield from self.content.types(subject, "Tag")
        for name, value in (("tagId", tag.id), ("tagName", tag.name), ("tagQuestionCount", tag.question_count)):
            yield from self.content.literal(subject, name, value)

    def map(self, events: Iterable[SharedEvent]) -> Iterator[Quad]:
        """Map balanced shared-record events to their owning named graphs."""
        active: SharedStart | None = None
        current: PostRecord | None = None
        for event in events:
            if isinstance(event, SharedStart):
                if active is not None or not event.graphs:
                    raise ValueError("Invalid shared record start")
                active, current = event, None
                if event.tag is not None:
                    yield from routed(self._tag(event.tag), event.graphs, self.content)
            elif isinstance(event, SharedEnd):
                if active is None:
                    raise ValueError("Unmatched shared record end")
                active, current = None, None
            else:
                if active is None:
                    raise ValueError("Shared content without active ownership")
                name = active.tag.name if active.tag is not None else None
                if isinstance(event, PostEvent):
                    current = event.post
                    if current.post_type not in (4, 5):
                        raise ValueError("Shared post must be tag documentation")
                    yield from routed(self.content.post(current, event.votes, name), active.graphs, self.content)
                    if active.tag is not None:
                        expected = active.tag.wiki_id if current.post_type == 5 else active.tag.excerpt_id
                        if current.id != expected:
                            raise ValueError("Tag documentation reference does not match")
                        relation = "hasTagWiki" if current.post_type == 5 else "hasTagExcerpt"
                        yield from routed(
                            self.content.relation(
                                self.content.iris.tag(name or ""), relation, self.content.iris.post(current, name)
                            ),
                            active.graphs,
                            self.content,
                        )
                elif isinstance(event, CommentEvent):
                    if current is None or event.parent.id != current.id:
                        raise ValueError("Documentation comment parent does not match")
                    yield from routed(
                        self.content.comment(event.comment, event.parent, self.content.iris.post(current, name)),
                        active.graphs,
                        self.content,
                    )
        if active is not None:
            raise ValueError("Incomplete shared record event stream")
