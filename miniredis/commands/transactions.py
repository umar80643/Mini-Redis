"""MULTI/EXEC/DISCARD state transitions for a single connection.

Queuing itself (append to `conn.tx` while `conn.multi` is set) happens in
`MiniRedisServer.execute`; this module only owns the three state-machine
transitions, kept separate so they're trivially unit-testable.
"""

from __future__ import annotations


def begin(conn) -> None:
    """Start a transaction. Raises if one is already open (no nesting)."""
    if conn.multi:
        raise RuntimeError("MULTI calls can not be nested")
    conn.multi = True
    conn.tx = []


def discard(conn) -> None:
    """Abandon the open transaction's queued commands."""
    if not conn.multi:
        raise RuntimeError("DISCARD without MULTI")
    conn.multi = False
    conn.tx = []


def take_queue(conn) -> list[list[bytes]]:
    """End the transaction and return its queued commands for execution."""
    if not conn.multi:
        raise RuntimeError("EXEC without MULTI")
    queued = conn.tx
    conn.multi = False
    conn.tx = []
    return queued
