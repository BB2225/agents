"""
http_retry.py – HTTP 425 retry decorator for Polymarket CLOB engine restarts.
Pause quoting, retry with backoff, resume cleanly.
"""
from __future__ import annotations

import asyncio
import functools
import logging
import time
from typing import Any, Callable, TypeVar

log = logging.getLogger("lean_bot")
F = TypeVar("F", bound=Callable[..., Any])

_restart_until: float = 0.0
RESTART_PAUSE_SEC = 5.0


def is_in_restart() -> bool:
    return time.time() < _restart_until


def retry_on_425(max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 16.0):
    """Sync decorator: retry on HTTP 425."""
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            global _restart_until
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if _is_425(e):
                        _restart_until = time.time() + RESTART_PAUSE_SEC
                        log.warning(f"[HTTP_425] {func.__name__} attempt {attempt}/{max_retries}, retry in {delay:.1f}s")
                        if attempt < max_retries:
                            time.sleep(delay)
                            delay = min(delay * 2, max_delay)
                            continue
                    raise
        return wrapper  # type: ignore
    return decorator


def async_retry_on_425(max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 16.0):
    """Async decorator: retry on HTTP 425."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            global _restart_until
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if _is_425(e):
                        _restart_until = time.time() + RESTART_PAUSE_SEC
                        log.warning(f"[HTTP_425] {func.__name__} attempt {attempt}/{max_retries}, retry in {delay:.1f}s")
                        if attempt < max_retries:
                            await asyncio.sleep(delay)
                            delay = min(delay * 2, max_delay)
                            continue
                    raise
        return wrapper
    return decorator


def _is_425(exc: Exception) -> bool:
    s = str(exc)
    for attr in ("status_code", "status", "code"):
        if getattr(exc, attr, None) == 425:
            return True
    return "425" in s or "too early" in s.lower()
