"""Attach registry language matches to source question records."""

from selection.languages import LanguageSelector
from vector.domain.models import QuestionRecord, SelectedQuestion


def select_question(question: QuestionRecord, selector: LanguageSelector) -> SelectedQuestion | None:
    """Return the question with matching languages, or None if unselected."""
    languages = selector.match(question.tags)
    return SelectedQuestion(question, languages) if languages else None
