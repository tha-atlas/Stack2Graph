"""Build reviewed HTTP IRIs with a bounded cache of encoded names."""

from collections import OrderedDict
from urllib.parse import quote

from graph.domain.source import PostRecord, PostReference

BASE = "http://stackoverflow.com/"
NAMESPACE = BASE + "schema#"


class IriPolicy:
    def __init__(self, cache_size: int = 8192) -> None:
        """Initialize a bounded cache of percent-encoded names."""
        if cache_size < 1:
            raise ValueError("IRI cache size must be positive")
        self.cache_size = cache_size
        self._encoded: OrderedDict[str, str] = OrderedDict()

    def _name(self, name: str) -> str:
        """Validate and percent-encode a name using the bounded cache."""
        if not name or any(ord(character) < 32 or ord(character) == 127 for character in name):
            raise ValueError("IRI name must be nonempty and contain no control characters")
        encoded = self._encoded.get(name)
        if encoded is None:
            encoded = quote(name, safe="")
            self._encoded[name] = encoded
            if len(self._encoded) > self.cache_size:
                self._encoded.popitem(last=False)
        else:
            self._encoded.move_to_end(name)
        return encoded

    @staticmethod
    def _id(value: int) -> int:
        """Require a positive signed-64-bit source identifier."""
        if not 0 < value < (1 << 63):
            raise ValueError(f"Invalid source identity: {value}")
        return value

    def question(self, source_id: int) -> str:
        """Return the canonical question IRI for a valid source ID."""
        return BASE + f"q/{self._id(source_id)}"

    def answer(self, source_id: int) -> str:
        """Return the canonical answer IRI for a valid source ID."""
        return BASE + f"a/{self._id(source_id)}"

    def tag(self, name: str) -> str:
        """Return the canonical tag IRI with a percent-encoded name."""
        return BASE + "tags/" + self._name(name)

    def post(self, post: PostRecord, tag_name: str | None = None) -> str:
        """Choose a question, answer, or documentation IRI for a supported post."""
        if post.post_type == 1:
            return self.question(post.id)
        if post.post_type == 2:
            return self.answer(post.id)
        if post.post_type == 5 and tag_name is not None:
            return self.tag(tag_name) + "/info"
        if post.post_type in (4, 5):
            return BASE + f"posts/{self._id(post.id)}/revisions"
        raise ValueError(f"Unsupported post type: {post.post_type}")

    def comment(self, source_id: int, parent: PostReference, parent_iri: str) -> str:
        """Build a comment IRI anchored to its question or documentation parent."""
        if not parent.exists:
            raise ValueError("Comment parent does not exist")
        if parent.post_type in (1, 2):
            question_id = parent.id if parent.post_type == 1 else parent.parent_id
            if question_id is None:
                raise ValueError("Answer comment requires its question identity")
            parent_iri = BASE + f"questions/{self._id(question_id)}"
        return parent_iri + f"#comment{self._id(source_id)}_{self._id(parent.id)}"

    def graph(self, key: str) -> str:
        """Return a canonical graph IRI for a flat category key."""
        if key in (".", "..") or "/" in key or "\\" in key:
            raise ValueError("Graph key must be a flat category")
        return BASE + self._name(key)
