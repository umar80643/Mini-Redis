"""Thin wrappers around `time` so call sites read as intent (monotonic vs wall) rather than
requiring the reader to remember which `time.*` function means what."""

from __future__ import annotations

import time


def monotonic() -> float:
    """Seconds from an arbitrary, ever-increasing clock. Use for durations and TTL math."""
    return time.monotonic()


def wall_time() -> float:
    """Seconds since the Unix epoch. Use for anything persisted or shown to a human."""
    return time.time()
