"""Define shared retry delays using zero-based attempt indices."""

_DELAY_SECONDS = (0.25, 0.5, 1.0, 2.0)


def retry_delay(retry_index: int) -> float:
    """Return the wait after the given zero-based transient retry index."""
    return _DELAY_SECONDS[min(retry_index, 3)]


def archive_http_delay(attempt: int) -> float:
    """Return the capped doubling wait for a zero-based archive.org retry."""
    return float(min(1 << attempt, 4))
