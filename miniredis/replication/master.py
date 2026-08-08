"""Primary-side replication: accepting followers and streaming writes to them.

The wire protocol deliberately mirrors real Redis's shape (REPLCONF /
PSYNC / FULLRESYNC / CONTINUE) because it's a well-known, easy-to-explain
handshake, not because this implements Redis's actual byte format.

Handshake, from the primary's point of view:

    replica -> REPLCONF listening-port <port>   (informational; replied normally)
    replica -> PSYNC <replid> <offset>
    primary -> +FULLRESYNC <replid> <offset>\\r\\n   (if a full resync is needed)
               $<n>\\r\\n<n bytes of JSON snapshot>\\r\\n
           or -> +CONTINUE <replid>\\r\\n               (if the backlog covers `offset`)
    primary -> [live RESP command stream, forever]

After PSYNC, the connection stops behaving like an ordinary client
connection: the primary pushes commands to it via the exact same
bounded-queue-plus-pusher-task pattern used for Pub/Sub delivery (see
`server/tcp_server.py:push_messages`), and the only inbound traffic
expected from the replica is periodic `REPLCONF ACK <offset>` — which
gets no reply at all, since the replica isn't reading one.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from ..observability import metrics
from ..persistence import snapshot_bytes
from ..protocol import BulkString, SimpleString, command_bytes, encode

if TYPE_CHECKING:
    from ..server import Conn, MiniRedisServer

log = logging.getLogger("miniredis")


async def propagate(server: "MiniRedisServer", parts: list[bytes]) -> None:
    """Broadcast one write command to every attached replica and advance the offset.

    Called from `MiniRedisServer.persist`, i.e. exactly the same moment a
    command is appended to the AOF — replication and AOF durability are
    driven off the same event, so they can never silently disagree about
    which writes "count."
    """
    state = server.replication
    if state.role != "primary":
        return

    encoded = command_bytes(parts)
    state.append_backlog(encoded)
    metrics.REPL_MASTER_OFFSET.set(state.master_repl_offset)

    for handle in list(state.replicas.values()):
        try:
            handle.queue.put_nowait(encoded)
        except asyncio.QueueFull:
            # A replica that can't keep up gets dropped rather than let a slow
            # follower apply backpressure to every client write on the primary.
            log.warning(
                "replica_queue_full_disconnecting",
                extra={"event": "replica_queue_full_disconnecting", "peer": handle.addr},
            )
            handle.conn.closing = True
            handle.conn.writer.close()


async def handle_replconf(server: "MiniRedisServer", conn: "Conn", args: list[bytes]):
    """REPLCONF subcommand handler. Returns a RESP reply, or None to send no reply at all."""
    if not args:
        return server.err("ERR wrong number of arguments for 'replconf' command")

    subcommand = args[0].upper()
    if subcommand == b"LISTENING-PORT" and len(args) == 2:
        conn.replica_listening_port = int(args[1])
        return SimpleString(b"OK")

    if subcommand == b"ACK" and len(args) == 2:
        handle = server.replication.replicas.get(conn)
        if handle is not None:
            handle.ack_offset = int(args[1])
            handle.last_ack_at = time.monotonic()
        return None  # the replica isn't reading a reply to its heartbeat ACK

    return server.err("ERR unknown REPLCONF subcommand")


async def handle_psync(server: "MiniRedisServer", conn: "Conn", args: list[bytes]):
    """PSYNC handshake. Writes the resync response directly to `conn` and returns
    None (the dispatcher's normal reply-writing path is bypassed for this command).
    """
    if len(args) != 2:
        return server.err("ERR wrong number of arguments for 'psync' command")

    state = server.replication
    requested_replid = args[0].decode(errors="replace")
    try:
        requested_offset = int(args[1])
    except ValueError:
        requested_offset = -1

    writer = conn.writer
    can_partial = (
        requested_replid == state.replid
        and requested_offset >= 0
        and state.backlog_covers(requested_offset)
    )

    if can_partial:
        writer.write(encode(SimpleString(f"CONTINUE {state.replid}".encode())))
        catch_up = state.backlog_from(requested_offset)
        start_offset = requested_offset
    else:
        writer.write(
            encode(SimpleString(f"FULLRESYNC {state.replid} {state.master_repl_offset}".encode()))
        )
        async with server.store.lock:
            payload = snapshot_bytes(server.store)
        writer.write(encode(BulkString(payload)))
        catch_up = b""
        start_offset = state.master_repl_offset
    await writer.drain()

    queue: "asyncio.Queue[bytes]" = asyncio.Queue(server.s.repl_replica_queue_size)
    from .state import ReplicaHandle  # local import: avoids a state<->master import cycle

    handle = ReplicaHandle(conn=conn, queue=queue, addr=conn.peer, ack_offset=start_offset)
    state.replicas[conn] = handle
    metrics.REPL_CONNECTED_REPLICAS.set(len(state.replicas))

    if catch_up:
        writer.write(catch_up)
        await writer.drain()

    handle.pusher_task = asyncio.create_task(_push_to_replica(handle))
    log.info(
        "replica_attached",
        extra={
            "event": "replica_attached",
            "peer": conn.peer,
            "command": "partial" if can_partial else "full",
        },
    )
    return None


async def _push_to_replica(handle) -> None:
    """Drain `handle`'s queue and forward each command's raw bytes to its socket.

    Deliberately the same shape as Pub/Sub's `push_messages`: one dedicated
    task per destination, reading from a bounded queue, so a slow replica
    can never block command execution on the primary.
    """
    try:
        while True:
            chunk = await handle.queue.get()
            handle.conn.writer.write(chunk)
            await handle.conn.writer.drain()
    except (ConnectionError, OSError, asyncio.CancelledError):
        return


def detach_replica(server: "MiniRedisServer", conn: "Conn") -> None:
    """Clean up a departed replica connection: cancel its pusher task and forget it."""
    handle = server.replication.replicas.pop(conn, None)
    if handle is not None and handle.pusher_task is not None:
        handle.pusher_task.cancel()
    metrics.REPL_CONNECTED_REPLICAS.set(len(server.replication.replicas))


def replication_info(server: "MiniRedisServer") -> list[str]:
    """The `# Replication` section of INFO, matching the shape real Redis uses."""
    state = server.replication
    lines = ["# Replication", f"role:{'master' if state.role == 'primary' else 'slave'}"]

    if state.role == "primary":
        lines.append(f"connected_slaves:{len(state.replicas)}")
        for index, handle in enumerate(state.replicas.values()):
            lag = max(0, state.master_repl_offset - handle.ack_offset)
            lines.append(f"slave{index}:addr={handle.addr},offset={handle.ack_offset},lag={lag}")
        lines.append(f"master_replid:{state.replid}")
        lines.append(f"master_repl_offset:{state.master_repl_offset}")
    else:
        lines.append(f"master_host:{state.master_host}")
        lines.append(f"master_port:{state.master_port}")
        lines.append(f"master_link_status:{state.link_status}")
        lines.append(f"master_replid:{state.master_replid or ''}")
        lines.append(f"slave_repl_offset:{state.replica_offset}")
        lines.append(f"master_repl_offset:{state.replica_offset}")

    return lines
