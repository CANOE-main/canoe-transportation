"""Helpers shared by source acquisition adapters."""

import logging
import time
from collections.abc import Callable
from typing import TypeVar

import requests


_LOGGER = logging.getLogger(__name__)
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_TRANSFER_RETRY_DELAYS = (0.5, 1.0)
_T = TypeVar("_T")


def retry_download(transfer: Callable[[], _T], *, url: str) -> _T:
    """Retry an idempotent transfer only after transient requests failures."""
    for delay in _TRANSFER_RETRY_DELAYS:
        try:
            return transfer()
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError,
                requests.exceptions.ChunkedEncodingError) as exc:
            status = exc.response.status_code if exc.response is not None else None
            if (isinstance(exc, requests.exceptions.SSLError)
                    or isinstance(exc, requests.HTTPError) and status not in _RETRYABLE_STATUS):
                raise
            retry_after = exc.response.headers.get("Retry-After", "") if exc.response is not None else ""
            if retry_after.isdecimal():
                delay = min(10.0, max(delay, int(retry_after)))
            _LOGGER.warning("Retrying download from %s after %s%s", url, type(exc).__name__,
                            f" (HTTP {status})" if status is not None else "")
            time.sleep(delay)
    return transfer()
