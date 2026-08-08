"""TTL tracking via a versioned min-heap.

A plain `heapq` keyed on expiry time is simple but leaks stale entries
whenever a key's TTL is changed or the key is overwritten: the old heap
record still exists, pointing at a key that may no longer be expiring
at that time (or may no longer exist at all). Rather than searching the
heap to remove the stale record (O(n)), each entry also carries the
version of the `Entry` it referred to when scheduled. When popped, the
consumer checks the record's version against the key's *current*
version and discards it if they differ — an O(log n) push and O(1)
staleness check instead of an O(n) delete.
"""

from __future__ import annotations

import heapq
from typing import Iterator

from .entry import Entry


class ExpirationIndex:
    """A min-heap of `(expires_at, version, key)` tuples ordered by expiry time."""

    def __init__(self) -> None:
        self.heap: list[tuple[float, int, bytes]] = []

    def schedule(self, key: bytes, entry: Entry) -> None:
        """Record `entry`'s expiry time, if it has one."""
        if entry.expires_at is not None:
            heapq.heappush(self.heap, (entry.expires_at, entry.version, key))

    def clear(self) -> None:
        self.heap.clear()

    def due(self, now: float) -> Iterator[tuple[float, int, bytes]]:
        """Pop and yield every heap record whose expiry time has passed.

        Callers must still verify each yielded record's version against the
        key's live entry before deleting — a popped record may be stale.
        """
        while self.heap and self.heap[0][0] <= now:
            yield heapq.heappop(self.heap)
