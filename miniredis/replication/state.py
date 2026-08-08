"""Replication state shared between the master-side and replica-side logic.

One `ReplicationState` instance lives on `MiniRedisServer.replication` for
the whole process lifetime, even across `REPLICAOF` role changes — a node
can be a primary with replicas attached, then be told `REPLICAOF <host>
<port>` and become a replica itself, without restarting.
"""

from __future__ import annotations

import asyncio
import secrets
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from ..server import Conn

Role = Literal["primary", "replica"]
LinkStatus = Literal["connect", "connecting", "sync", "connected"]


def generate_replid() -> str:
    """A 40-hex-char run ID, same shape as Redis's, identifying one data lineage.

    A replica compares the replid a primary offers during PSYNC against the
    replid it last synced from: a match means "same lineage, my offset is
    still meaningful, a partial resync might work"; a mismatch means the
    primary restarted/changed identity and only a full resync is safe.
    """
    return secrets.token_hex(20)


@dataclass(slots=True)
class ReplicaHandle:
    """Primary-side bookkeeping for one attached replica connection."""

    conn: "Conn"
    queue: "asyncio.Queue[bytes]"
    addr: str
    ack_offset: int = 0
    connected_at: float = field(default_factory=time.monotonic)
    last_ack_at: float = field(default_factory=time.monotonic)
    pusher_task: "asyncio.Task | None" = None


@dataclass(slots=True)
class ReplicationState:
    """Everything needed to answer "what is this node's replication role and status."""

    role: Role = "primary"
    replid: str = field(default_factory=generate_replid)

    # Primary side: how many bytes of write-command stream have been produced,
    # plus a bounded backlog of the most recent ones for partial resync.
    master_repl_offset: int = 0
    backlog: list[tuple[int, bytes]] = field(default_factory=list)
    backlog_bytes: int = 0
    backlog_limit: int = 1_048_576
    replicas: dict["Conn", ReplicaHandle] = field(default_factory=dict)

    # Replica side: where we're following from.
    master_host: str | None = None
    master_port: int | None = None
    master_replid: str | None = None
    link_status: LinkStatus = "connect"
    replica_offset: int = 0
    link_task: "asyncio.Task | None" = None
    last_io_at: float = field(default_factory=time.monotonic)

    def is_replica_conn(self, conn: "Conn") -> bool:
        return conn in self.replicas

    def append_backlog(self, encoded: bytes) -> None:
        """Record `encoded` (already-serialized command bytes) for potential partial resync."""
        start_offset = self.master_repl_offset
        self.backlog.append((start_offset, encoded))
        self.backlog_bytes += len(encoded)
        self.master_repl_offset += len(encoded)
        while self.backlog_bytes > self.backlog_limit and len(self.backlog) > 1:
            _, dropped = self.backlog.pop(0)
            self.backlog_bytes -= len(dropped)

    def backlog_covers(self, offset: int) -> bool:
        """Whether a replica resuming from `offset` can be served from the backlog
        alone (partial resync), rather than needing a full snapshot resync."""
        if not self.backlog:
            return offset == self.master_repl_offset
        return self.backlog[0][0] <= offset <= self.master_repl_offset

    def backlog_from(self, offset: int) -> bytes:
        """Concatenated command bytes needed to bring a replica from `offset` up to date."""
        return b"".join(chunk for start, chunk in self.backlog if start >= offset)

    def min_replica_ack(self) -> int | None:
        """The slowest attached replica's acknowledged offset, or None if no replicas."""
        if not self.replicas:
            return None
        return min(handle.ack_offset for handle in self.replicas.values())
