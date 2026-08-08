"""In-memory storage engine: the single source of truth for keyspace state.

Command handlers never touch a raw dict — everything goes through this
class so that memory accounting, eviction, expiration, and locking stay
consistent regardless of which command triggered the mutation.

Concurrency: `asyncio.Lock` serializes all reads and writes. This makes
every public method here atomic with respect to every other one, which
is what lets `INCR` be a correct, race-free counter under concurrent
clients despite Python's cooperative scheduling.
"""

from __future__ import annotations

import asyncio
import fnmatch
import time

from .entry import Entry
from .eviction import select_victim
from .expiration import ExpirationIndex
from .memory import estimate_entry_size


class OutOfMemory(Exception):
    """Raised when a write would exceed `max_memory` under the `noeviction` policy."""


class Storage:
    """A single-database, string-only keyspace with TTLs and bounded memory."""

    def __init__(self, max_memory: int = 256 * 1024 * 1024, policy: str = "allkeys-lru") -> None:
        self.data: dict[bytes, Entry] = {}
        self.expirations = ExpirationIndex()
        self.heap = self.expirations.heap  # exposed for tests/introspection
        self.lock = asyncio.Lock()
        self.max_memory = max_memory
        self.policy = policy

        self.used = 0
        self.peak = 0
        self.expired = 0
        self.evicted = 0

    # -- internal helpers (caller already holds self.lock) -----------------

    def _is_expired(self, key: bytes, entry: Entry) -> bool:
        """Lazily reap `key` if its TTL has passed; report whether it was reaped."""
        if entry.expires_at is not None and entry.expires_at <= time.monotonic():
            self._remove(key)
            self.expired += 1
            return True
        return False

    def _remove(self, key: bytes) -> Entry | None:
        entry = self.data.pop(key, None)
        if entry is not None:
            self.used -= entry.approximate_size
        return entry

    def _touch(self, entry: Entry) -> None:
        entry.last_accessed = time.monotonic()
        entry.access_count += 1

    def _make_room(self, needed_bytes: int) -> None:
        """Evict entries until `needed_bytes` more would fit under `max_memory`."""
        while self.used + needed_bytes > self.max_memory and self.data:
            if self.policy == "noeviction":
                raise OutOfMemory
            victim = select_victim(self.data, self.policy)
            self._remove(victim)
            self.evicted += 1
        if self.used + needed_bytes > self.max_memory:
            raise OutOfMemory

    # -- public API ----------------------------------------------------------

    async def set(
        self,
        key: bytes,
        value: bytes,
        ttl_ms: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Set `key` to `value`, honoring NX/XX semantics. Returns whether the write happened."""
        async with self.lock:
            existing = self.data.get(key)
            if existing is not None and self._is_expired(key, existing):
                existing = None

            if nx and existing is not None:
                return False
            if xx and existing is None:
                return False

            previous_version = existing.version if existing is not None else 0
            if existing is not None:
                self._remove(key)

            now = time.monotonic()
            expires_at = now + ttl_ms / 1000 if ttl_ms is not None else None
            size = estimate_entry_size(key, value)

            entry = Entry(
                value=value,
                created_at=now,
                expires_at=expires_at,
                last_accessed=now,
                access_count=0,
                version=previous_version + 1,
                approximate_size=size,
            )
            self._make_room(size)
            self.data[key] = entry
            self.used += size
            self.peak = max(self.peak, self.used)
            self.expirations.schedule(key, entry)
            return True

    async def get(self, key: bytes, touch: bool = True) -> bytes | None:
        """Return the value for `key`, or None if absent/expired."""
        async with self.lock:
            entry = self.data.get(key)
            if entry is None or self._is_expired(key, entry):
                return None
            if touch:
                self._touch(entry)
            return entry.value

    async def delete(self, *keys: bytes) -> int:
        """Delete the given keys; returns how many actually existed."""
        async with self.lock:
            return sum(1 for key in keys if self._remove(key) is not None)

    async def exists(self, *keys: bytes) -> int:
        """Count how many of the given keys are present (and not expired)."""
        found = 0
        for key in keys:
            if await self.get(key, touch=False) is not None:
                found += 1
        return found

    async def expire(self, key: bytes, ttl_ms: int) -> int:
        """Set a TTL (in ms) on `key`. Returns 1 if applied, 0 if the key is absent."""
        async with self.lock:
            entry = self.data.get(key)
            if entry is None or self._is_expired(key, entry):
                return 0
            entry.version += 1
            entry.expires_at = time.monotonic() + ttl_ms / 1000
            self.expirations.schedule(key, entry)
            return 1

    async def persist(self, key: bytes) -> int:
        """Remove any TTL on `key`. Returns 1 if a TTL was cleared, else 0."""
        async with self.lock:
            entry = self.data.get(key)
            if entry is None or self._is_expired(key, entry) or entry.expires_at is None:
                return 0
            entry.version += 1
            entry.expires_at = None
            return 1

    async def ttl(self, key: bytes, ms: bool = False) -> int:
        """Return remaining TTL: -2 if absent, -1 if no TTL, else time left (sec or ms)."""
        async with self.lock:
            entry = self.data.get(key)
            if entry is None or self._is_expired(key, entry):
                return -2
            if entry.expires_at is None:
                return -1
            remaining = max(0.0, entry.expires_at - time.monotonic())
            return int(remaining * 1000) if ms else int(remaining)

    async def incr(self, key: bytes, delta: int) -> int:
        """Atomically add `delta` to the integer stored at `key` (default 0) and return it."""
        async with self.lock:
            entry = self.data.get(key)
            if entry is not None and self._is_expired(key, entry):
                entry = None

            current = int(entry.value) if entry is not None else 0
            new_value = current + delta
            if new_value < -(2**63) or new_value > 2**63 - 1:
                raise OverflowError("increment or decrement would overflow a 64-bit integer")

            encoded = str(new_value).encode()
            if entry is not None:
                self.used -= entry.approximate_size
                entry.value = encoded
                entry.approximate_size = estimate_entry_size(key, encoded)
                self.used += entry.approximate_size
                self._touch(entry)
            else:
                now = time.monotonic()
                size = estimate_entry_size(key, encoded)
                self._make_room(size)
                entry = Entry(
                    value=encoded,
                    created_at=now,
                    expires_at=None,
                    last_accessed=now,
                    access_count=1,
                    version=1,
                    approximate_size=size,
                )
                self.data[key] = entry
                self.used += size

            self.peak = max(self.peak, self.used)
            return new_value

    async def flush(self) -> None:
        """Remove every key."""
        async with self.lock:
            self.data.clear()
            self.expirations.clear()
            self.used = 0

    async def keys(self, pattern: bytes = b"*") -> list[bytes]:
        """Return all live keys matching a glob-style `pattern`, reaping expired ones first."""
        async with self.lock:
            for key, entry in list(self.data.items()):
                self._is_expired(key, entry)
            decoded_pattern = pattern.decode(errors="replace")
            return [
                k for k in self.data if fnmatch.fnmatch(k.decode(errors="replace"), decoded_pattern)
            ]

    async def expire_cycle(self) -> int:
        """Actively reap keys whose TTL has passed; returns how many were removed.

        Called periodically by the server's background expirer task. Each
        heap entry is checked against the key's current `version` and
        `expires_at` so that a stale heap record left behind by a since
        -overwritten or TTL-changed key is silently discarded rather than
        wrongly deleting live data.
        """
        async with self.lock:
            now = time.monotonic()
            removed = 0
            for expires_at, version, key in self.expirations.due(now):
                entry = self.data.get(key)
                if (
                    entry is not None
                    and entry.version == version
                    and entry.expires_at == expires_at
                ):
                    self._remove(key)
                    self.expired += 1
                    removed += 1
            return removed

    async def dump_commands(self) -> list[list[bytes]]:
        """Reconstruct the current keyspace as a minimal list of SET commands.

        Used by AOF rewrite (compact the log down to current state) and by
        anything else that needs "the commands that would recreate this
        exact keyspace" rather than a JSON blob.
        """
        async with self.lock:
            now = time.monotonic()
            commands: list[list[bytes]] = []
            for key, entry in self.data.items():
                if entry.expires_at is not None and entry.expires_at <= now:
                    continue
                command = [b"SET", key, entry.value]
                if entry.expires_at is not None:
                    remaining_ms = max(0, int((entry.expires_at - now) * 1000))
                    absolute_ms = str(int(time.time() * 1000) + remaining_ms).encode()
                    command += [b"PXAT", absolute_ms]
                commands.append(command)
            return commands
