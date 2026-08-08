"""Durability: an append-only command log (AOF) plus atomic point-in-time snapshots.

Two independent mechanisms, matching how Redis itself layers durability:

- AOF: every write command is appended as it happens; replaying it from
  an empty keyspace reconstructs exact history. Cheap per-write, but the
  file only grows — see `AOF.rewrite` for compaction.
- Snapshot: a full point-in-time dump of the keyspace, serialized as
  JSON. Cheap to load (no replay), used together with an AOF reset
  (SAVE, or on clean shutdown) to keep the AOF from growing unbounded.
  The same in-memory serialization (`snapshot_bytes`/`apply_snapshot_bytes`)
  also backs replication's full resync, so "give a follower my current
  state" and "write my current state to disk" share one code path.

On startup the server loads the snapshot first, then replays whatever
AOF records were appended after that snapshot was taken.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from pathlib import Path
from typing import IO

from ..protocol import NeedMoreData, RESPError, command_bytes, parse_command

log = logging.getLogger("miniredis")

SNAPSHOT_FORMAT_VERSION = 1


class AOF:
    """An append-only file of RESP-encoded commands, with configurable fsync durability."""

    def __init__(self, path: str, mode: str = "everysec", enabled: bool = True):
        if mode not in {"always", "everysec", "no"}:
            raise ValueError("AOF fsync must be always, everysec, or no")
        self.path = Path(path)
        self.mode = mode
        self.enabled = enabled
        self.lock = asyncio.Lock()
        self.fp: IO[bytes] | None = None
        self._fsync_task: asyncio.Task | None = None
        self.writes = 0
        self.rewrites = 0

    async def open(self) -> None:
        if not self.enabled:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fp = open(self.path, "ab", buffering=0)
        if self.mode == "everysec":
            self._fsync_task = asyncio.create_task(self._fsync_loop())

    async def _fsync_loop(self) -> None:
        """Background task for `everysec` mode: batch fsyncs once per second.

        Trade-off: `always` fsyncs every write (safest, slowest — bounds
        data loss to zero committed writes but adds a syscall per command).
        `everysec` batches fsyncs on a timer (loses at most ~1s of writes
        on a crash, much higher throughput). `no` never explicitly fsyncs
        and relies on the OS to flush its page cache eventually (fastest,
        weakest guarantee — a crash, not just a process exit, can lose
        more than a second of writes).
        """
        try:
            while True:
                await asyncio.sleep(1)
                async with self.lock:
                    if self.fp:
                        os.fsync(self.fp.fileno())
        except asyncio.CancelledError:
            return

    async def append(self, parts: list[bytes]) -> None:
        """Append one command. Fsyncs immediately in `always` mode; otherwise deferred."""
        if not self.enabled:
            return
        if self.fp is None:
            raise RuntimeError("AOF is not open")
        encoded = command_bytes(parts)
        async with self.lock:
            self.fp.write(encoded)
            self.writes += 1
            if self.mode == "always":
                os.fsync(self.fp.fileno())

    async def flush(self) -> None:
        async with self.lock:
            if self.fp:
                self.fp.flush()
                os.fsync(self.fp.fileno())

    async def close(self) -> None:
        if self._fsync_task:
            self._fsync_task.cancel()
            await asyncio.gather(self._fsync_task, return_exceptions=True)
            self._fsync_task = None
        await self.flush()
        if self.fp:
            self.fp.close()
            self.fp = None

    async def reset(self) -> None:
        """Truncate the AOF to empty, typically right after a fresh snapshot is durable."""
        if not self.enabled:
            return
        async with self.lock:
            if self.fp:
                self.fp.close()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_bytes(b"")
            self.fp = open(self.path, "ab", buffering=0)

    def read_commands(self) -> list[list[bytes]]:
        """Parse every complete command currently in the AOF file, for startup replay."""
        if not self.enabled or not self.path.exists():
            return []
        data = self.path.read_bytes()
        pos = 0
        commands = []
        while pos < len(data):
            try:
                command, consumed = parse_command(data[pos:])
                commands.append(command)
                pos += consumed
            except NeedMoreData:
                break  # crash-truncated final record is safely ignored
            except RESPError as exc:
                raise RuntimeError(f"AOF corruption at offset {pos}") from exc
        return commands

    def size_bytes(self) -> int:
        """Current on-disk AOF size, or 0 if disabled/not yet created."""
        if not self.enabled or not self.path.exists():
            return 0
        return self.path.stat().st_size

    async def rewrite(self, storage) -> float:
        """Compact the AOF: replace the full mutation history with the minimal set
        of commands that reconstructs the current keyspace (one SET per live key).

        This is the same trade-off Redis's BGREWRITEAOF makes: a long-running
        server accumulates an AOF far larger than its actual dataset (every
        INCR, every overwritten SET is still in the log). Rewriting collapses
        that history down to current state, then atomically swaps it in via a
        temp-file-plus-rename so a crash mid-rewrite can never corrupt or lose
        the existing AOF — the old file stays valid until the new one is fully
        written, fsynced, and renamed over it.

        Returns the rewrite duration in seconds.
        """
        if not self.enabled:
            return 0.0

        started = time.perf_counter()
        commands = await storage.dump_commands()

        tmp_path = self.path.with_suffix(self.path.suffix + ".rewrite.tmp")
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        payload = b"".join(command_bytes(command) for command in commands)

        with open(tmp_path, "wb") as fp:
            fp.write(payload)
            fp.flush()
            os.fsync(fp.fileno())

        async with self.lock:
            if self.fp:
                self.fp.close()
            os.replace(tmp_path, self.path)
            self.fp = open(self.path, "ab", buffering=0)
            self.rewrites += 1

        return time.perf_counter() - started


def snapshot_bytes(storage) -> bytes:
    """Serialize `storage`'s current keyspace to JSON bytes, in memory.

    Shared by `save_snapshot` (persists to disk) and replication's full
    resync (sends the same bytes straight over the wire to a new replica).
    Must be called while already holding `storage.lock` — see both callers.
    """
    monotonic_now = time.monotonic()
    wall_now = time.time()
    rows = []
    for key, entry in storage.data.items():
        ttl = None if entry.expires_at is None else max(0, entry.expires_at - monotonic_now)
        rows.append(
            {
                "k": base64.b64encode(key).decode(),
                "v": base64.b64encode(entry.value).decode(),
                "expires_unix": None if ttl is None else wall_now + ttl,
            }
        )
    payload = {"version": SNAPSHOT_FORMAT_VERSION, "created_unix": wall_now, "entries": rows}
    return json.dumps(payload, separators=(",", ":")).encode()


async def apply_snapshot_bytes(storage, data: bytes) -> int:
    """Load JSON snapshot bytes (from `snapshot_bytes`) into `storage`. Returns keys loaded.

    Does not flush `storage` first — callers that want a clean load (disk
    recovery, replica full resync) must call `storage.flush()` beforehand.
    """
    raw = json.loads(data)
    rows = raw.get("entries", raw) if isinstance(raw, dict) else raw
    now = time.time()
    loaded = 0
    for row in rows:
        ttl_ms = None if row["expires_unix"] is None else int((row["expires_unix"] - now) * 1000)
        if ttl_ms is not None and ttl_ms <= 0:
            continue  # already expired between snapshot and load; skip rather than resurrect
        await storage.set(base64.b64decode(row["k"]), base64.b64decode(row["v"]), ttl_ms)
        loaded += 1
    return loaded


async def save_snapshot(storage, path: str) -> None:
    """Write a full, crash-consistent snapshot of `storage` to `path`.

    Durability is achieved by writing to a temp file, fsyncing its
    contents, then atomically renaming it over the destination — a
    process crash or power loss can never leave `path` holding a
    partially-written file. Directory fsync is attempted afterward so the
    rename itself survives a crash where the platform supports it.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")

    async with storage.lock:
        payload = snapshot_bytes(storage)

    try:
        with open(tmp, "wb") as fp:
            fp.write(payload)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp, target)
    except OSError:
        # Preserve the last known-good snapshot and remove an incomplete temp file.
        tmp.unlink(missing_ok=True)
        log.exception("snapshot_write_failed", extra={"event": "snapshot_write_failed"})
        raise

    directory_flag = getattr(os, "O_DIRECTORY", None)
    if directory_flag is None:
        log.debug("directory_fsync_unsupported", extra={"event": "directory_fsync_unsupported"})
        return

    dir_fd = None
    try:
        dir_fd = os.open(str(target.parent), directory_flag)
        os.fsync(dir_fd)
    except OSError as exc:
        # The snapshot is already atomically installed; some filesystems/platforms
        # simply don't permit directory fsync. Record the reduced durability guarantee.
        log.warning(
            "directory_fsync_failed",
            extra={"event": "directory_fsync_failed", "command": type(exc).__name__},
        )
    finally:
        if dir_fd is not None:
            os.close(dir_fd)


async def load_snapshot(storage, path: str) -> int:
    """Load a snapshot written by `save_snapshot` back into `storage`. Returns keys loaded."""
    target = Path(path)
    if not target.exists():
        return 0
    return await apply_snapshot_bytes(storage, target.read_bytes())
