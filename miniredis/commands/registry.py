"""Static metadata for every supported command: arity and read/write kind.

`MiniRedisServer.execute` uses this to reject malformed requests (unknown
command, wrong arg count) before they ever reach `dispatch()`, and the
`COMMAND` command reports it back to clients. Handler-specific arity
variants (e.g. INCR vs INCRBY) are still checked inside the handler
itself, since a single `(min_args, max_args)` pair can't express "1 arg
for INCR, 2 for INCRBY."
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CommandSpec:
    name: str
    min_args: int
    max_args: int | None
    write: bool = False
    transaction_compatible: bool = True
    pubsub_compatible: bool = False


_SPECS = [
    CommandSpec("PING", 0, 1),
    CommandSpec("ECHO", 1, 1),
    CommandSpec("SET", 2, None, write=True),
    CommandSpec("GET", 1, 1),
    CommandSpec("GETDEL", 1, 1, write=True),
    CommandSpec("MGET", 1, None),
    CommandSpec("MSET", 2, None, write=True),
    CommandSpec("DEL", 1, None, write=True),
    CommandSpec("EXISTS", 1, None),
    CommandSpec("TYPE", 1, 1),
    CommandSpec("INCR", 1, 1, write=True),
    CommandSpec("DECR", 1, 1, write=True),
    CommandSpec("INCRBY", 2, 2, write=True),
    CommandSpec("DECRBY", 2, 2, write=True),
    CommandSpec("EXPIRE", 2, 2, write=True),
    CommandSpec("PEXPIRE", 2, 2, write=True),
    CommandSpec("PEXPIREAT", 2, 2, write=True),
    CommandSpec("TTL", 1, 1),
    CommandSpec("PTTL", 1, 1),
    CommandSpec("PERSIST", 1, 1, write=True),
    CommandSpec("DBSIZE", 0, 0),
    CommandSpec("FLUSHDB", 0, 0, write=True),
    CommandSpec("KEYS", 1, 1),
    CommandSpec("SCAN", 1, None),
    CommandSpec("INFO", 0, 1),
    CommandSpec("COMMAND", 0, 0),
    CommandSpec("SAVE", 0, 0),
    CommandSpec("MULTI", 0, 0),
    CommandSpec("EXEC", 0, 0),
    CommandSpec("DISCARD", 0, 0),
    CommandSpec("SUBSCRIBE", 1, None, pubsub_compatible=True),
    CommandSpec("UNSUBSCRIBE", 0, None, pubsub_compatible=True),
    CommandSpec("PUBLISH", 2, 2, pubsub_compatible=True),
    CommandSpec("REPLCONF", 1, None),
    CommandSpec("PSYNC", 2, 2),
    CommandSpec("REPLICAOF", 2, 2),
    CommandSpec("BGREWRITEAOF", 0, 0),
]

COMMANDS: dict[str, CommandSpec] = {spec.name: spec for spec in _SPECS}
