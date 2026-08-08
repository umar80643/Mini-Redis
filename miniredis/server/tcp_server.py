"""The async TCP server: connection lifecycle, framing, and per-command bookkeeping.

This module owns *networking* concerns only — reading bytes off the
wire, feeding them through the incremental RESP parser, and writing
replies back. Command semantics are delegated entirely to
`commands.dispatcher.dispatch`; this file never inspects command
arguments beyond checking arity against the command registry.
"""

from __future__ import annotations

import asyncio
import logging
import time

from ..commands import COMMANDS
from ..commands.dispatcher import dispatch
from ..observability import metrics
from ..persistence import AOF, load_snapshot, save_snapshot
from ..protocol import (
    Array,
    BulkString,
    Error,
    NeedMoreData,
    RESPError,
    SimpleString,
    encode,
    parse_command,
)
from ..pubsub import Broker
from ..replication.master import detach_replica, propagate
from ..replication.replica import run_replica_link
from ..replication.state import ReplicationState
from ..storage import OutOfMemory, Storage
from .connection import Conn
from .lifecycle import ExecutionGate

log = logging.getLogger("miniredis")


class MiniRedisServer:
    """Owns the storage engine, AOF, snapshot recovery, Pub/Sub broker, and TCP listener."""

    def __init__(self, settings):
        self.s = settings
        self.store = Storage(settings.max_memory_mb * 1024 * 1024, settings.eviction_policy)
        self.aof = AOF(settings.aof_path, settings.aof_fsync, settings.aof_enabled)
        self.broker = Broker()
        self.server: asyncio.base_events.Server | None = None
        self.clients: set[Conn] = set()
        self.client_tasks: set[asyncio.Task] = set()
        self.started = time.monotonic()
        self.ready = False
        self.gate = ExecutionGate()
        self.exp_task: asyncio.Task | None = None

        if settings.replica_of:
            host, _, port = settings.replica_of.partition(":")
            self.replication = ReplicationState(
                role="replica", master_host=host, master_port=int(port)
            )
        else:
            self.replication = ReplicationState(role="primary")
        self.replication.backlog_limit = settings.repl_backlog_bytes

        # Aggregate counters surfaced via INFO/metrics
        self.total_connections = 0
        self.total_commands = 0
        self.protocol_errors = 0
        self.get_hits = 0
        self.get_misses = 0
        self._metric_expired = 0
        self._metric_evicted = 0

    # -- startup / shutdown ---------------------------------------------------

    async def start(self) -> None:
        """Recover state from disk (snapshot + AOF replay), then start accepting connections."""
        recovery_started = time.perf_counter()
        snapshot_keys = await load_snapshot(self.store, self.s.snapshot_path)

        aof_commands = self.aof.read_commands()
        for command in aof_commands:
            await self.execute(None, command, replay=True)

        await self.aof.open()
        self.ready = True
        self.exp_task = asyncio.create_task(self.expirer())

        log.info(
            "recovery_complete",
            extra={
                "event": "recovery_complete",
                "duration_ms": round((time.perf_counter() - recovery_started) * 1000, 3),
                "command": f"snapshot_keys={snapshot_keys},aof_commands={len(aof_commands)}",
            },
        )

        if self.s.metrics_enabled:
            metrics.start(self.s.metrics_port)

        self.server = await asyncio.start_server(
            self.handle, self.s.miniredis_host, self.s.miniredis_port
        )
        log.info("MiniRedis listening on %s:%s", self.s.miniredis_host, self.s.miniredis_port)

        if self.replication.role == "replica":
            self.replication.link_task = asyncio.create_task(run_replica_link(self))

    async def close(self) -> None:
        """Stop accepting connections, drain existing clients, and persist final state."""
        log.info("shutdown_started", extra={"event": "shutdown_started"})
        self.ready = False

        if self.server:
            self.server.close()

        if self.exp_task:
            self.exp_task.cancel()
            await asyncio.gather(self.exp_task, return_exceptions=True)

        if self.replication.link_task:
            self.replication.link_task.cancel()
            await asyncio.gather(self.replication.link_task, return_exceptions=True)
        for handle in list(self.replication.replicas.values()):
            if handle.pusher_task:
                handle.pusher_task.cancel()

        for conn in list(self.clients):
            conn.closing = True
            conn.writer.close()

        # Wait for connection handlers rather than StreamWriter.wait_closed() directly.
        # This lets each handler run its own cleanup (Pub/Sub unsubscribe, metrics, etc.)
        # and gives shutdown a hard upper bound via shutdown_timeout.
        pending = [task for task in self.client_tasks if not task.done()]
        if pending:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending, return_exceptions=True), self.s.shutdown_timeout
                )
            except asyncio.TimeoutError:
                log.warning("client_shutdown_timeout", extra={"event": "client_shutdown_timeout"})
                for task in pending:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        if self.server:
            await self.server.wait_closed()

        if self.s.snapshot_enabled:
            await save_snapshot(self.store, self.s.snapshot_path)
            await self.aof.reset()
        await self.aof.close()

        log.info("shutdown_complete", extra={"event": "shutdown_complete"})

    async def expirer(self) -> None:
        """Background task: actively reap expired keys every 100ms.

        Lazy expiration (on GET/etc.) already hides expired keys from
        clients immediately; this loop exists so memory is reclaimed and
        `expired_keys` metrics advance even for keys nobody reads again.
        """
        while True:
            expired = await self.store.expire_cycle()
            if expired:
                log.debug(
                    "expiration_cycle",
                    extra={"event": "expiration_cycle", "command": f"expired={expired}"},
                )
            await asyncio.sleep(0.1)

    # -- connection handling ----------------------------------------------------

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Per-connection loop: read, parse, dispatch, write — until the client disconnects."""
        task = asyncio.current_task()
        if task is not None:
            self.client_tasks.add(task)

        if len(self.clients) >= self.s.max_connections:
            writer.close()
            await writer.wait_closed()
            return

        conn = Conn(
            writer,
            peer=str(writer.get_extra_info("peername")),
            queue=asyncio.Queue(self.s.pubsub_queue_size),
        )
        self.clients.add(conn)
        self.total_connections += 1
        metrics.CLIENTS.inc()
        metrics.CONNECTIONS.inc()
        log.info(
            "client_connected",
            extra={"event": "client_connected", "connection_id": conn.id, "peer": conn.peer},
        )

        buf = b""
        push_task = asyncio.create_task(self.push_messages(conn))
        try:
            while True:
                try:
                    chunk = await asyncio.wait_for(reader.read(65536), self.s.client_idle_timeout)
                except asyncio.TimeoutError:
                    break
                if not chunk:
                    break

                conn.bytes_read += len(chunk)
                buf += chunk
                if len(buf) > self.s.max_request_bytes:
                    raise RESPError("request too large")

                buf = await self._drain_commands(conn, writer, buf)
        except (RESPError, ConnectionError, BrokenPipeError) as exc:
            if isinstance(exc, RESPError):
                metrics.PROTOCOL_ERRORS.inc()
                self.protocol_errors += 1
            try:
                writer.write(encode(Error(f"ERR {exc}".encode())))
                await writer.drain()
            except (ConnectionError, BrokenPipeError, OSError):
                log.debug("client write/close failed", exc_info=True)
        finally:
            push_task.cancel()
            self.broker.unsubscribe(conn)
            detach_replica(self, conn)
            self.clients.discard(conn)
            metrics.CLIENTS.dec()
            writer.close()
            log.info(
                "client_disconnected",
                extra={"event": "client_disconnected", "connection_id": conn.id},
            )
            try:
                await asyncio.wait_for(writer.wait_closed(), 0.5)
            except (asyncio.TimeoutError, ConnectionError, BrokenPipeError, OSError):
                log.debug("client write/close failed", exc_info=True)
            finally:
                if task is not None:
                    self.client_tasks.discard(task)

    async def _drain_commands(self, conn: Conn, writer: asyncio.StreamWriter, buf: bytes) -> bytes:
        """Parse and execute every complete command currently in `buf`; return the remainder."""
        while buf:
            try:
                parts, consumed = parse_command(
                    buf, max_bulk=self.s.max_bulk_string_bytes, max_array=self.s.max_array_length
                )
            except NeedMoreData:
                break
            buf = buf[consumed:]

            is_exec = parts[0].upper() == b"EXEC"
            if is_exec:
                await self.gate.enter_exclusive()
                try:
                    response = await self.execute(conn, parts)
                finally:
                    await self.gate.exit_exclusive()
            else:
                await self.gate.enter_shared()
                try:
                    response = await self.execute(conn, parts)
                finally:
                    await self.gate.exit_shared()

            if response is not None:
                payload = encode(response)
                writer.write(payload)
                await writer.drain()
                conn.bytes_written += len(payload)
            conn.commands_processed += 1
        return buf

    async def push_messages(self, conn: Conn) -> None:
        """Deliver queued Pub/Sub messages to `conn` as they arrive."""
        while True:
            channel, message = await conn.queue.get()
            payload = encode(
                Array([BulkString(b"message"), BulkString(channel), BulkString(message)])
            )
            conn.writer.write(payload)
            await conn.writer.drain()
            conn.bytes_written += len(payload)

    # -- command execution --------------------------------------------------------

    def err(self, message: str) -> Error:
        return Error(message.encode())

    async def execute(self, conn: Conn | None, parts: list[bytes], replay: bool = False):
        """Validate a parsed command against the registry, then dispatch it.

        Centralizes what's common to every command: unknown-command and
        arity checks, MULTI queuing, metrics/latency recording, and
        translating a few expected exceptions (bad integer, OOM) into
        RESP errors instead of letting them crash the connection.
        """
        cmd = parts[0].decode(errors="replace").upper()
        args = parts[1:]
        started_at = time.perf_counter()

        spec = COMMANDS.get(cmd)
        if spec is None:
            metrics.COMMANDS.labels(cmd, "error").inc()
            return self.err(f"ERR unknown command '{cmd.lower()}'")

        if len(args) < spec.min_args or (spec.max_args is not None and len(args) > spec.max_args):
            metrics.COMMANDS.labels(cmd, "error").inc()
            return self.err(f"ERR wrong number of arguments for '{cmd.lower()}' command")

        # A replica only accepts writes from its master link (conn is None for those —
        # see replication/replica.py). A write from a real client socket is rejected,
        # matching Redis's read-only-replica behavior.
        if conn is not None and spec.write and self.replication.role == "replica":
            metrics.COMMANDS.labels(cmd, "error").inc()
            return self.err("READONLY You can't write against a read only replica.")

        if conn is not None and conn.multi and cmd not in {"EXEC", "DISCARD", "MULTI"}:
            conn.tx.append(parts)
            return SimpleString(b"QUEUED")

        try:
            response = await dispatch(self, conn, cmd, args, replay)
            self.total_commands += 1
            metrics.COMMANDS.labels(cmd, "success").inc()
            return response
        except (ValueError, OverflowError):
            metrics.COMMANDS.labels(cmd, "error").inc()
            return self.err("ERR value is not an integer or out of range")
        except OutOfMemory:
            metrics.COMMANDS.labels(cmd, "error").inc()
            return self.err("OOM command not allowed when used memory > maxmemory")
        finally:
            duration = time.perf_counter() - started_at
            metrics.LATENCY.labels(cmd).observe(duration)
            log.debug(
                "command_executed",
                extra={
                    "event": "command_executed",
                    "connection_id": getattr(conn, "id", None),
                    "command": cmd,
                    "duration_ms": round(duration * 1000, 3),
                },
            )
            metrics.KEYS.set(len(self.store.data))
            metrics.MEMORY.set(self.store.used)
            metrics.PEAK_MEMORY.set(self.store.peak)
            if self.store.expired > self._metric_expired:
                metrics.EXPIRED.inc(self.store.expired - self._metric_expired)
                self._metric_expired = self.store.expired
            if self.store.evicted > self._metric_evicted:
                metrics.EVICTED.inc(self.store.evicted - self._metric_evicted)
                self._metric_evicted = self.store.evicted

    async def persist(self, parts: list[bytes], replay: bool) -> None:
        """Append a command to the AOF and propagate it to replicas, unless it's
        being replayed from the AOF itself (replay=True at startup) or arrived
        from our own master link (a replica never re-propagates downstream —
        no replica-of-replica chaining, see replication/replica.py).
        """
        if not replay:
            await self.aof.append(parts)
            if self.aof.enabled:
                metrics.AOF_WRITES.inc()
            await propagate(self, parts)
