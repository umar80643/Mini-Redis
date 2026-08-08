"""Eviction victim selection for bounded-memory operation.

Kept deliberately separate from `Storage` so the eviction policy can be
unit-tested and swapped without touching locking or memory-accounting logic.
"""

from __future__ import annotations

from .entry import Entry

SUPPORTED_POLICIES = {"noeviction", "allkeys-lru", "allkeys-lfu"}


def select_victim(data: dict[bytes, Entry], policy: str) -> bytes:
    """Return the key that should be evicted next under `policy`.

    - allkeys-lfu: evict the least-frequently-used key, breaking ties by
      least-recently-used (a plain hit-count comparison alone would let a
      key that was popular long ago block eviction forever).
    - allkeys-lru (and any other non-lfu policy): evict the least-recently-used key.
    """
    if policy == "allkeys-lfu":
        return min(data, key=lambda k: (data[k].access_count, data[k].last_accessed))
    return min(data, key=lambda k: data[k].last_accessed)
