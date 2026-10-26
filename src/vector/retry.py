"""Retry transient Qdrant and network failures with bounded delays."""

import time
from collections.abc import Callable
from enum import Enum
from typing import Protocol, runtime_checkable

import httpx
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

from utils.backoff import retry_delay


@runtime_checkable
class _RPCFailure(Protocol):
    def code(self) -> Enum:
        """Return the RPC status code for retry classification."""
        ...


def transient(error: Exception) -> bool:
    """Return whether an HTTP, RPC, or network error permits a retry."""
    if isinstance(error, UnexpectedResponse):
        return error.status_code in (408, 429, 500, 502, 503, 504)
    if isinstance(error, ResponseHandlingException):
        return transient(error.source)
    if isinstance(error, _RPCFailure):
        return error.code().name in ("UNAVAILABLE", "RESOURCE_EXHAUSTED", "DEADLINE_EXCEEDED", "ABORTED")
    return isinstance(
        error,
        (TimeoutError, ConnectionError, httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError),
    )


def retry_operation[T](
    operation: Callable[[], T],
    attempts: int,
    on_retry: Callable[[], None] | None = None,
    *,
    deadline: float | None = None,
) -> T:
    """
    Retry transient failures with backoff within an optional deadline.

    Args:
        operation: Callable to invoke again after a transient failure.
        attempts: Maximum total number of calls, including the first call.
        on_retry: Optional callback before each retry delay.
        deadline: Absolute time.monotonic() deadline, or None for no deadline.

    Returns:
        The first successful operation result.

    Raises:
        TimeoutError: The deadline expires before an operation starts.
        Exception: A failure is nontransient or the final attempt fails.

    """
    for attempt in range(attempts):
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("Operation deadline exceeded")
        try:
            return operation()
        except Exception as error:
            if not transient(error) or attempt + 1 == attempts:
                raise
            if on_retry is not None:
                on_retry()
            delay = retry_delay(attempt)
            time.sleep(delay if deadline is None else min(delay, max(0.0, deadline - time.monotonic())))
    raise RuntimeError("Operation attempts exhausted")
