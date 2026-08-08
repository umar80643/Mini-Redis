"""Shared fixtures for replication tests.

These tests deliberately use real TCP (via port=0 / ephemeral ports)
rather than calling `execute()` directly against two in-process objects,
because the thing under test *is* the wire handshake and streaming
protocol between two servers — a mocked transport wouldn't exercise it.
"""

from __future__ import annotations

import asyncio

import pytest

from miniredis.config import Settings
from miniredis.server import Conn, MiniRedisServer


async def wait_until(predicate, timeout: float = 3.0, interval: float = 0.02) -> None:
    """Poll `predicate` (a zero-arg callable, may be async) until truthy or timeout."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")


def _settings(tmp_path, name: str, **overrides) -> Settings:
    defaults = dict(
        aof_enabled=False,
        snapshot_enabled=False,
        snapshot_path=str(tmp_path / f"{name}-dump.json"),
        metrics_enabled=False,
        miniredis_port=0,
        repl_reconnect_delay=0.2,
        repl_read_timeout=0.3,
        repl_ack_interval=0.2,
    )
    defaults.update(overrides)
    return Settings(**defaults)


@pytest.fixture
async def primary(tmp_path):
    server = MiniRedisServer(_settings(tmp_path, "primary"))
    await server.start()
    yield server
    await server.close()


@pytest.fixture
async def synced_pair(tmp_path, primary):
    """A primary and a replica already attached and past initial full resync."""
    primary_port = primary.server.sockets[0].getsockname()[1]
    replica = MiniRedisServer(
        _settings(tmp_path, "replica", replica_of=f"127.0.0.1:{primary_port}")
    )
    await replica.start()
    await wait_until(lambda: replica.replication.link_status == "connected")
    yield primary, replica
    await replica.close()


def fake_conn() -> Conn:
    """A `Conn` with a no-op writer, for calling `execute()` as if from a real client
    without needing an actual socket."""

    class _Writer:
        def write(self, _data):
            pass

        async def drain(self):
            pass

    return Conn(_Writer())
