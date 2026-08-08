"""Approximate per-entry memory accounting, used for `maxmemory` enforcement."""

from __future__ import annotations

# Rough allowance for Entry's Python object overhead (dataclass slots, dict entry, etc).
# Not exact - CPython's real allocator overhead varies by build - but stable and cheap,
# which matters more here than precision: eviction only needs a consistent ordering.
ENTRY_METADATA_BYTES = 96


def estimate_entry_size(key: bytes, value: bytes) -> int:
    """Approximate logical memory used by one entry (key + value + fixed overhead)."""
    return len(key) + len(value) + ENTRY_METADATA_BYTES
