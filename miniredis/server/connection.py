"""Per-client connection state, tracked separately from the raw asyncio streams."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field


@dataclass(eq=False, slots=True)
class ConnectionState:
    """Everything the dispatcher and server need to know about one client.

    `eq=False` gives instances identity-based equality/hashing, which is
    what lets a `ConnectionState` be used directly as a dict/set key (see
    `pubsub.Broker.by_conn`) without needing a custom `__hash__`.
    """

    writer: asyncio.StreamWriter
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    peer: str = ""
    selected_db: int = 0

    # MULTI/EXEC transaction state
    multi: bool = False
    tx: list[list[bytes]] = field(default_factory=list)

    # Pub/Sub delivery queue; bounded so a slow subscriber can't grow unbounded memory
    queue: "asyncio.Queue[tuple[bytes, bytes]]" = field(default_factory=lambda: asyncio.Queue(256))

    # Per-connection counters, surfaced via INFO/metrics
    bytes_read: int = 0
    bytes_written: int = 0
    commands_processed: int = 0

    authenticated: bool = True
    closing: bool = False
    replica_listening_port: int | None = None


Conn = ConnectionState
