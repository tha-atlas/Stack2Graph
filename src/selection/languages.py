"""Cache per-tag language matches within each registry's size limit."""

from functools import lru_cache

from selection.models import SelectionRegistry


class LanguageSelector:
    def __init__(self, registry: SelectionRegistry, *, cache_size: int = 8192) -> None:
        """Set up tag matching with a separate cache for this registry."""
        if cache_size < 1:
            raise ValueError("Selection cache size must be positive")
        self.registry = registry
        self.cache_size = cache_size
        self._base = frozenset(registry.base_tags)
        self._aliases = dict(registry.synonyms)
        # Wrap a bound method per instance: registries never share cache entries.
        self._cached_match = lru_cache(maxsize=cache_size)(self._match_tag)

    @property
    def cached_tags(self) -> int:
        """The number of tag matches currently cached."""
        return self._cached_match.cache_info().currsize

    def _match_tag(self, tag: str) -> tuple[str, ...]:
        """Return sorted languages matching a base tag, version, or alias."""
        matches = {tag} if tag in self._base else set[str]()
        matches.update(language for language, pattern in self.registry.versions if pattern.fullmatch(tag))
        alias = self._aliases.get(tag)
        if alias is not None:
            matches.add(alias)
        return tuple(sorted(matches))

    def match(self, tags: tuple[str, ...]) -> tuple[str, ...]:
        """Return the sorted, unique languages matching the supplied tags."""
        return tuple(sorted({language for tag in tags for language in self._cached_match(tag)}))
