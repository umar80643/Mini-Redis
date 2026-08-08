"""Replica-side replication: following a primary and applying its write stream.

`run_replica_link` is a single long-lived background task per replica node
(started by `become_replica`, cancelled by `stop_replica`). It owns one
outbound TCP connection to the primary and this state machine:

    connect -> handshake (REPLCONF, PSYNC) -> apply FULLRESYNC/CONTINUE
    -> loop { read commands, apply locally, periodically send REPLCONF ACK }
    -> on any error, back off and reconnect from the top

Reconnect is intentionally simple (fixed delay, no exponential backoff or
jitter) because the assignment is to keep this whiteboard-explainable, not
to build a production reconnect policy.
"""

from __future__ import annotations

import asyncio
import logging
import time

from ..protocol import NeedMoreData, RESPError, command_bytes, parse_command
from .state import ReplicationState

log = logging.getLogger("miniredis")


async def _read_line(reader: asyncio.StreamReader) -> bytes:
    line = await reader.readline()
    if not line:
        raise ConnectionError("master closed connection during handshake")
    return line.rstrip(b"\r\n")


async def _do_handshake(
    server, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> bytes:
    """Run REPLCONF + PSYNC against a freshly connected primary.

    Returns any already-buffered live-stream bytes that were read past the
    handshake response (the primary may start pushing writes immediately
    after the resync payload, in the same TCP segment).
    """
    state = server.replication

    writer.write(
        command_bytes([b"REPLCONF", b"listening-port", str(server.s.miniredis_port).encode()])
    )
    await writer.drain()
    await _read_line(reader)  # discard the "+OK\r\n" reply

    writer.write(
        command_bytes([b"PSYNC", state.replid.encode(), str(state.replica_offset).encode()])
    )
    await writer.drain()
    state.link_status = "sync"

    header = await _read_line(reader)  # b"+FULLRESYNC <replid> <offset>" or b"+CONTINUE <replid>"
    parts = header.lstrip(b"+").split(b" ")
    kind = parts[0]

    if kind == b"FULLRESYNC":
        state.master_replid = parts[1].decode()
        state.replica_offset = int(parts[2])

        length_line = await _read_line(reader)  # b"$<n>"
        length = int(length_line[1:])
        payload = b""
        while len(payload) < length + 2:  # +2 for the trailing CRLF after the bulk string
            chunk = await reader.read(length + 2 - len(payload))
            if not chunk:
                raise ConnectionError("master closed connection mid-snapshot")
            payload += chunk
        snapshot = payload[:length]

        from ..persistence import apply_snapshot_bytes

        await server.store.flush()
        await apply_snapshot_bytes(server.store, snapshot)
        log.info(
            "replica_full_resync",
            extra={"event": "replica_full_resync", "command": f"offset={state.replica_offset}"},
        )
    else:  # CONTINUE: backlog already covered our offset, nothing to load
        state.master_replid = parts[1].decode()
        log.info("replica_partial_resync", extra={"event": "replica_partial_resync"})

    state.link_status = "connected"
    return b""


async def _stream_loop(server, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """After a successful handshake: apply the live command stream and send ACKs."""
    state = server.replication
    buf = b""
    last_ack = time.monotonic()

    while True:
        try:
            chunk = await asyncio.wait_for(reader.read(65536), server.s.repl_read_timeout)
        except asyncio.TimeoutError:
            chunk = b""
        else:
            if not chunk:
                raise ConnectionError("master closed the replication stream")

        state.last_io_at = time.monotonic()
        buf += chunk

        while True:
            try:
                parts, consumed = parse_command(buf)
            except NeedMoreData:
                break
            except RESPError as exc:
                raise ConnectionError(f"corrupt replication stream: {exc}") from exc
            buf = buf[consumed:]
            # replay=False: a replica durably persists what it applies to its own AOF too,
            # it just never re-propagates it further (no replica-of-replica chaining).
            await server.execute(None, parts, replay=False)
            state.replica_offset += consumed

        if time.monotonic() - last_ack >= server.s.repl_ack_interval:
            writer.write(command_bytes([b"REPLCONF", b"ACK", str(state.replica_offset).encode()]))
            await writer.drain()
            last_ack = time.monotonic()


async def run_replica_link(server) -> None:
    """Top-level reconnect loop for following `server.replication.master_host/port`."""
    state = server.replication
    while state.role == "replica":
        writer = None
        try:
            state.link_status = "connecting"
            reader, writer = await asyncio.open_connection(state.master_host, state.master_port)
            await _do_handshake(server, reader, writer)
            await _stream_loop(server, reader, writer)
        except asyncio.CancelledError:
            raise
        except (ConnectionError, OSError, ValueError) as exc:
            log.warning(
                "replica_link_error",
                extra={"event": "replica_link_error", "command": str(exc)},
            )
            state.link_status = "connect"
            await asyncio.sleep(server.s.repl_reconnect_delay)
        finally:
            if writer is not None:
                writer.close()


def become_replica(server, host: str, port: int) -> None:
    """Switch this node into replica role, following `host:port`."""
    stop_replica(server)
    server.replication = ReplicationState(role="replica", master_host=host, master_port=port)
    server.replication.link_task = asyncio.create_task(run_replica_link(server))


def stop_replica(server) -> None:
    """Cancel any active replication link and revert to being an ordinary primary."""
    state = server.replication
    if state.link_task is not None:
        state.link_task.cancel()
    server.replication = ReplicationState(role="primary")
