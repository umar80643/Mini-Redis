"""The stored representation of a single key's value and bookkeeping metadata."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Entry:
    """One keyspace entry.

    `version` is bumped on every mutation (SET, EXPIRE, PERSIST, INCR) and
    is what lets the TTL heap (see `expiration.py`) detect stale records
    without an O(n) search. `access_count` / `last_accessed` back LRU/LFU
    eviction (see `eviction.py`).
    """

    value: bytes
    created_at: float
    expires_at: float | None = None
    last_accessed: float = 0.0
    access_count: int = 0
    version: int = 0
    approximate_size: int = 0
