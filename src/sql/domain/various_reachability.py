"""Record traversal depth and link counts for questions outside selection."""

from dataclasses import dataclass

from utils.types import JSONObject


@dataclass(frozen=True)
class DepthCounts:
    depth: int
    links: int
    source_questions: int
    various_links: int
    new_questions: int
    selected_target_links: int
    missing_target_links: int
    non_question_links: int

    @property
    def repeat_links(self) -> int:
        """Edges beyond first discovery, including cycles and converging paths."""
        return self.various_links - self.new_questions

    def to_json(self) -> JSONObject:
        """Serialize per-depth link classifications and newly reached questions."""
        return {
            "depth": self.depth,
            "links": self.links,
            "source_questions_with_links": self.source_questions,
            "various_links": self.various_links,
            "new_various_questions": self.new_questions,
            "repeat_or_converging_links": self.repeat_links,
            "selected_target_links": self.selected_target_links,
            "missing_target_links": self.missing_target_links,
            "non_question_target_links": self.non_question_links,
        }


@dataclass(frozen=True)
class ReachabilityResult:
    language: str
    levels: tuple[DepthCounts, ...]

    @property
    def unique_questions(self) -> int:
        """The total number of newly reached various-language questions."""
        return sum(level.new_questions for level in self.levels)

    @property
    def max_depth(self) -> int:
        """The deepest level with newly reached questions, or zero."""
        return max((level.depth for level in self.levels if level.new_questions), default=0)

    def to_json(self) -> JSONObject:
        """Serialize traversal totals and per-depth counts."""
        return {
            "complete": True,
            "direct_various_links": self.levels[0].various_links if self.levels else 0,
            "unique_various_questions": self.unique_questions,
            "max_various_depth": self.max_depth,
            "various_links_examined": sum(level.various_links for level in self.levels),
            "levels": [level.to_json() for level in self.levels],
        }
