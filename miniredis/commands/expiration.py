"""Helpers for normalizing EXPIRE/PEXPIRE/PEXPIREAT into relative milliseconds."""

from __future__ import annotations

import time


def expiration_ms(command: str, raw: int) -> int:
    """Convert an EXPIRE-family argument into a relative TTL in milliseconds."""
    if command == "EXPIRE":
        return raw * 1000
    if command == "PEXPIREAT":
        return max(0, raw - int(time.time() * 1000))
    return raw  # PEXPIRE: already relative milliseconds


def absolute_expiry_ms(ttl_ms: int) -> bytes:
    """Convert a relative TTL (ms) into an absolute unix-ms timestamp, for AOF persistence.

    Persisting the absolute deadline rather than the relative TTL means
    replaying the AOF after a restart expires the key at the *original*
    intended time, not `ttl_ms` after the restart.
    """
    return str(int(time.time() * 1000) + ttl_ms).encode()
