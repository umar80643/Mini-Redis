"""Small Pub/Sub helper shared by the SUBSCRIBE/UNSUBSCRIBE handlers."""

from __future__ import annotations


def subscription_count(broker, conn) -> int:
    """How many channels `conn` is currently subscribed to, via `broker`."""
    return len(broker.by_conn.get(conn, set()))
