"""Command dispatch: maps each command name to a small, focused handler.

Command *semantics* live here, deliberately separate from TCP framing
and connection lifecycle (see `server/tcp_server.py`). Each handler is
a plain async function with the signature
`(server, conn, args, replay) -> RESPValue`, so adding a command means
writing one function and registering it in `COMMAND_HANDLERS` below —
no new branch in a growing if/elif ladder.

`replay` is True while commands are being replayed from the AOF at
startup; handlers use it to skip re-appending to the AOF (see
`server.persist`) so recovery doesn't duplicate history.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Awaitable, Callable

from ..observability import metrics
from ..persistence import save_snapshot
from ..protocol import Array, BulkString, Integer, RESPValue, SimpleString
from ..replication.master import handle_psync, handle_replconf
from ..replication.replica import become_replica, stop_replica
from .expiration import absolute_expiry_ms, expiration_ms
from .generic import parse_scan_options
from .pubsub import subscription_count
from .registry import COMMANDS
from .server import build_info
from .strings import parse_set_options
from .transactions import begin as tx_begin
from .transactions import discard as tx_discard
from .transactions import take_queue

if TYPE_CHECKING:
    from ..server import Conn, MiniRedisServer

log = logging.getLogger("miniredis")

Handler = Callable[
    ["MiniRedisServer", "Conn | None", list[bytes], bool], Awaitable["RESPValue | None"]
]


def _wrong_args(server: "MiniRedisServer", command: str) -> RESPValue:
    return server.err(f"ERR wrong number of arguments for '{command.lower()}' command")


# -- connection / introspection ----------------------------------------------


async def cmd_ping(server, conn, args, replay):
    """PING [message] - respond PONG, or echo `message` if given."""
    if not args:
        return SimpleString(b"PONG")
    if len(args) != 1:
        return _wrong_args(server, "PING")
    return BulkString(args[0])


async def cmd_echo(server, conn, args, replay):
    """ECHO message - return `message` unchanged."""
    if len(args) != 1:
        return _wrong_args(server, "ECHO")
    return BulkString(args[0])


async def cmd_info(server, conn, args, replay):
    """INFO - return a human-readable server/keyspace status report."""
    return BulkString(build_info(server))


async def cmd_command(server, conn, args, replay):
    """COMMAND - list every supported command with its arity and read/write kind."""
    rows = [
        Array(
            [
                BulkString(name.encode()),
                Integer(spec.min_args),
                Integer(-1 if spec.max_args is None else spec.max_args),
                BulkString(b"write" if spec.write else b"read"),
            ]
        )
        for name, spec in sorted(COMMANDS.items())
    ]
    return Array(rows)


async def cmd_dbsize(server, conn, args, replay):
    """DBSIZE - number of live (non-expired) keys."""
    return Integer(len(await server.store.keys()))


# -- string / key commands ---------------------------------------------------


async def cmd_set(server, conn, args, replay):
    """SET key value [EX sec | PX ms | PXAT ms-unix] [NX | XX]."""
    if len(args) < 2:
        return _wrong_args(server, "SET")
    key, value = args[0], args[1]
    try:
        ttl_ms, nx, xx = parse_set_options(args[2:])
    except SyntaxError:
        return server.err("ERR syntax error")

    stored = await server.store.set(key, value, ttl_ms, nx, xx)
    if not stored:
        return BulkString(None)

    persisted = [b"SET", key, value]
    if ttl_ms is not None:
        persisted += [b"PXAT", absolute_expiry_ms(ttl_ms)]
    await server.persist(persisted, replay)
    return SimpleString(b"OK")


async def cmd_get(server, conn, args, replay):
    """GET key - return the value, or nil if absent/expired. Tracks hit/miss for cache metrics."""
    if len(args) != 1:
        return _wrong_args(server, "GET")
    value = await server.store.get(args[0])
    if value is not None:
        server.get_hits += 1
        metrics.GET_HITS.inc()
    else:
        server.get_misses += 1
        metrics.GET_MISSES.inc()
    return BulkString(value)


async def cmd_getdel(server, conn, args, replay):
    """GETDEL key - return the value and atomically delete the key."""
    if len(args) != 1:
        return _wrong_args(server, "GETDEL")
    value = await server.store.get(args[0])
    removed = await server.store.delete(args[0])
    if removed:
        await server.persist([b"DEL", args[0]], replay)
    return BulkString(value)


async def cmd_del(server, conn, args, replay):
    """DEL key [key ...] - delete the given keys; returns how many existed."""
    if not args:
        return _wrong_args(server, "DEL")
    removed = await server.store.delete(*args)
    await server.persist([b"DEL", *args], replay)
    return Integer(removed)


async def cmd_exists(server, conn, args, replay):
    """EXISTS key [key ...] - count how many of the given keys are present."""
    if not args:
        return _wrong_args(server, "EXISTS")
    return Integer(await server.store.exists(*args))


async def cmd_type(server, conn, args, replay):
    """TYPE key - 'string' if present, else 'none' (only strings are supported)."""
    if len(args) != 1:
        return _wrong_args(server, "TYPE")
    present = await server.store.get(args[0], touch=False) is not None
    return SimpleString(b"string" if present else b"none")


async def cmd_mget(server, conn, args, replay):
    """MGET key [key ...] - return each key's value, or nil for missing keys."""
    if not args:
        return _wrong_args(server, "MGET")
    return Array([BulkString(await server.store.get(key)) for key in args])


async def cmd_mset(server, conn, args, replay):
    """MSET key value [key value ...] - set multiple keys atomically-per-key."""
    if not args or len(args) % 2:
        return _wrong_args(server, "MSET")
    for i in range(0, len(args), 2):
        await server.store.set(args[i], args[i + 1])
    await server.persist([b"MSET", *args], replay)
    return SimpleString(b"OK")


async def cmd_incr_family(server, conn, args, replay, *, command: str):
    """Shared implementation for INCR / DECR / INCRBY / DECRBY."""
    by_amount = "BY" in command
    if len(args) != (2 if by_amount else 1):
        return _wrong_args(server, command)
    magnitude = int(args[1]) if by_amount else 1
    delta = -magnitude if command.startswith("DECR") else magnitude

    new_value = await server.store.incr(args[0], delta)
    await server.persist([b"SET", args[0], str(new_value).encode()], replay)
    return Integer(new_value)


# -- TTL / key metadata -------------------------------------------------------


async def cmd_expire_family(server, conn, args, replay, *, command: str):
    """Shared implementation for EXPIRE / PEXPIRE / PEXPIREAT."""
    if len(args) != 2:
        return _wrong_args(server, command)
    ttl_ms = expiration_ms(command, int(args[1]))
    applied = await server.store.expire(args[0], ttl_ms)
    if applied:
        await server.persist([b"PEXPIREAT", args[0], absolute_expiry_ms(ttl_ms)], replay)
    return Integer(applied)


async def cmd_ttl_family(server, conn, args, replay, *, command: str):
    """Shared implementation for TTL / PTTL."""
    if len(args) != 1:
        return _wrong_args(server, command)
    return Integer(await server.store.ttl(args[0], ms=(command == "PTTL")))


async def cmd_persist(server, conn, args, replay):
    """PERSIST key - remove any TTL, making the key permanent."""
    if len(args) != 1:
        return _wrong_args(server, "PERSIST")
    applied = await server.store.persist(args[0])
    if applied:
        await server.persist([b"PERSIST", args[0]], replay)
    return Integer(applied)


# -- keyspace-wide commands ---------------------------------------------------


async def cmd_flushdb(server, conn, args, replay):
    """FLUSHDB - remove every key."""
    await server.store.flush()
    await server.persist([b"FLUSHDB"], replay)
    return SimpleString(b"OK")


async def cmd_keys(server, conn, args, replay):
    """KEYS pattern - all live keys matching a glob pattern (O(n); use SCAN for large keyspaces)."""
    if len(args) != 1:
        return _wrong_args(server, "KEYS")
    return Array([BulkString(key) for key in await server.store.keys(args[0])])


async def cmd_scan(server, conn, args, replay):
    """SCAN cursor [MATCH pattern] [COUNT n] - cursor-based iteration over the keyspace."""
    if not args:
        return _wrong_args(server, "SCAN")
    cursor = int(args[0])
    try:
        count, pattern = parse_scan_options(args[1:])
    except SyntaxError:
        return server.err("ERR syntax error")

    matching_keys = await server.store.keys(pattern)
    page = matching_keys[cursor : cursor + count]
    next_cursor = 0 if cursor + count >= len(matching_keys) else cursor + count
    return Array([BulkString(str(next_cursor).encode()), Array([BulkString(k) for k in page])])


async def cmd_save(server, conn, args, replay):
    """SAVE - write a point-in-time snapshot to disk and truncate the AOF."""
    await save_snapshot(server.store, server.s.snapshot_path)
    await server.aof.reset()
    log.info("snapshot_saved", extra={"event": "snapshot_saved"})
    return SimpleString(b"OK")


# -- transactions --------------------------------------------------------------


async def cmd_multi(server, conn, args, replay):
    """MULTI - begin queuing subsequent commands for atomic EXEC."""
    if conn is None:
        return server.err("ERR MULTI unavailable")
    try:
        tx_begin(conn)
    except RuntimeError as exc:
        return server.err(f"ERR {exc}")
    return SimpleString(b"OK")


async def cmd_discard(server, conn, args, replay):
    """DISCARD - abandon a queued MULTI transaction without executing it."""
    if conn is None:
        return server.err("ERR DISCARD without MULTI")
    try:
        tx_discard(conn)
    except RuntimeError as exc:
        return server.err(f"ERR {exc}")
    return SimpleString(b"OK")


async def cmd_exec(server, conn, args, replay):
    """EXEC - run every command queued since MULTI, in order, and return their replies."""
    if conn is None:
        return server.err("ERR EXEC without MULTI")
    try:
        queued_commands = take_queue(conn)
    except RuntimeError as exc:
        return server.err(f"ERR {exc}")
    return Array([await server.execute(conn, queued, replay) for queued in queued_commands])


# -- pub/sub --------------------------------------------------------------------


async def cmd_subscribe(server, conn, args, replay):
    """SUBSCRIBE channel [channel ...] - subscribe this connection to one or more channels."""
    if conn is None or not args:
        return _wrong_args(server, "SUBSCRIBE")
    for channel in args:
        server.broker.subscribe(conn, channel)
    return Array(
        [
            BulkString(b"subscribe"),
            BulkString(args[-1]),
            Integer(subscription_count(server.broker, conn)),
        ]
    )


async def cmd_unsubscribe(server, conn, args, replay):
    """UNSUBSCRIBE [channel] - unsubscribe from one channel, or all if none given."""
    if conn is None:
        return server.err("ERR unavailable")
    channel = args[0] if args else None
    server.broker.unsubscribe(conn, channel)
    return Array(
        [
            BulkString(b"unsubscribe"),
            BulkString(args[0] if args else b""),
            Integer(subscription_count(server.broker, conn)),
        ]
    )


async def cmd_publish(server, conn, args, replay):
    """PUBLISH channel message - deliver `message` to every subscriber of `channel`."""
    delivered = await server.broker.publish(args[0], args[1])
    metrics.PUBSUB_MESSAGES.inc()
    return Integer(delivered)


# -- replication ------------------------------------------------------------------


async def cmd_replconf(server, conn, args, replay):
    """REPLCONF listening-port <port> | REPLCONF ACK <offset> - replication handshake/heartbeat.

    ACK gets no reply: the replica that sends it isn't reading one, it's
    already back in its read loop for the next batch of streamed commands.
    """
    if conn is None:
        return server.err("ERR REPLCONF requires a connection")
    return await handle_replconf(server, conn, args)


async def cmd_psync(server, conn, args, replay):
    """PSYNC replid offset - attach this connection as a replica and start streaming.

    Writes the resync response directly to the connection and switches it
    into replica-link mode; returns None so the normal per-command reply
    path is skipped for this one (see server/tcp_server.py's `_drain_commands`).
    """
    if conn is None:
        return server.err("ERR PSYNC requires a connection")
    return await handle_psync(server, conn, args)


async def cmd_replicaof(server, conn, args, replay):
    """REPLICAOF host port | REPLICAOF NO ONE - change this node's replication role at runtime."""
    if args[0].upper() == b"NO" and args[1].upper() == b"ONE":
        stop_replica(server)
        return SimpleString(b"OK")
    try:
        port = int(args[1])
    except ValueError:
        return server.err("ERR invalid master port")
    become_replica(server, args[0].decode(), port)
    return SimpleString(b"OK")


async def cmd_bgrewriteaof(server, conn, args, replay):
    """BGREWRITEAOF - compact the AOF down to the minimal commands for current state."""
    duration = await server.aof.rewrite(server.store)
    metrics.AOF_REWRITES.inc()
    metrics.AOF_REWRITE_DURATION.observe(duration)
    metrics.AOF_SIZE_BYTES.set(server.aof.size_bytes())
    return SimpleString(b"OK")


# -- dispatch table ------------------------------------------------------------

COMMAND_HANDLERS: dict[str, Handler] = {
    "PING": cmd_ping,
    "ECHO": cmd_echo,
    "INFO": cmd_info,
    "COMMAND": cmd_command,
    "DBSIZE": cmd_dbsize,
    "SET": cmd_set,
    "GET": cmd_get,
    "GETDEL": cmd_getdel,
    "DEL": cmd_del,
    "EXISTS": cmd_exists,
    "TYPE": cmd_type,
    "MGET": cmd_mget,
    "MSET": cmd_mset,
    "INCR": lambda s, c, a, r: cmd_incr_family(s, c, a, r, command="INCR"),
    "DECR": lambda s, c, a, r: cmd_incr_family(s, c, a, r, command="DECR"),
    "INCRBY": lambda s, c, a, r: cmd_incr_family(s, c, a, r, command="INCRBY"),
    "DECRBY": lambda s, c, a, r: cmd_incr_family(s, c, a, r, command="DECRBY"),
    "EXPIRE": lambda s, c, a, r: cmd_expire_family(s, c, a, r, command="EXPIRE"),
    "PEXPIRE": lambda s, c, a, r: cmd_expire_family(s, c, a, r, command="PEXPIRE"),
    "PEXPIREAT": lambda s, c, a, r: cmd_expire_family(s, c, a, r, command="PEXPIREAT"),
    "TTL": lambda s, c, a, r: cmd_ttl_family(s, c, a, r, command="TTL"),
    "PTTL": lambda s, c, a, r: cmd_ttl_family(s, c, a, r, command="PTTL"),
    "PERSIST": cmd_persist,
    "FLUSHDB": cmd_flushdb,
    "KEYS": cmd_keys,
    "SCAN": cmd_scan,
    "SAVE": cmd_save,
    "MULTI": cmd_multi,
    "DISCARD": cmd_discard,
    "EXEC": cmd_exec,
    "SUBSCRIBE": cmd_subscribe,
    "UNSUBSCRIBE": cmd_unsubscribe,
    "PUBLISH": cmd_publish,
    "REPLCONF": cmd_replconf,
    "PSYNC": cmd_psync,
    "REPLICAOF": cmd_replicaof,
    "BGREWRITEAOF": cmd_bgrewriteaof,
}


async def dispatch(
    server: "MiniRedisServer",
    conn: "Conn | None",
    cmd: str,
    args: list[bytes],
    replay: bool = False,
) -> RESPValue | None:
    """Look up and run the handler for `cmd`. Unknown commands return a RESP error.

    Returns None for the handful of replication commands (PSYNC, REPLCONF
    ACK) that either write their own reply directly to the connection or
    intentionally send no reply at all; the caller must skip the normal
    encode-and-write step in that case (see `server/tcp_server.py`).

    Arity bounds from `registry.COMMANDS` are already enforced by the
    caller (`MiniRedisServer.execute`); handlers here only check the
    finer-grained arg-count variants that a single registry entry can't
    express (e.g. INCR takes 1 arg but INCRBY takes 2).
    """
    handler = COMMAND_HANDLERS.get(cmd)
    if handler is None:
        return server.err(f"ERR unknown command '{cmd.lower()}'")
    return await handler(server, conn, args, replay)
