"""Concurrency behaviors not already covered by test_tcp_and_concurrency.py:
racing GET/SET, TTL expiring mid-access, MULTI/EXEC atomicity under
concurrent load, and a slow Pub/Sub subscriber not blocking publishers
or other subscribers.
"""

import asyncio

import pytest

from miniredis.config import Settings
from miniredis.protocol import Array, BulkString, Integer, SimpleString
from miniredis.server import Conn, MiniRedisServer


def _fake_conn() -> Conn:
    class _Writer:
        def write(self, _data):
            pass

        async def drain(self):
            pass

    return Conn(_Writer())


@pytest.fixture
async def app(tmp_path):
    settings = Settings(
        aof_enabled=False,
        snapshot_enabled=False,
        snapshot_path=str(tmp_path / "x"),
        metrics_enabled=False,
    )
    return MiniRedisServer(settings)


async def test_concurrent_get_set_never_sees_a_torn_value(app):
    """While many writers race to SET the same key, every GET must observe
    a value that was actually written in full - never a partial/corrupted one.
    The storage lock (see storage/engine.py) is what guarantees this."""
    written_values = {f"v{i}".encode() for i in range(20)}

    async def writer(i):
        await app.execute(None, [b"SET", b"race", f"v{i}".encode()])

    async def reader(results):
        for _ in range(50):
            reply = await app.execute(None, [b"GET", b"race"])
            if reply.value is not None:
                results.append(reply.value)

    results: list[bytes] = []
    await asyncio.gather(*(writer(i) for i in range(20)), reader(results))

    assert all(value in written_values for value in results)


async def test_ttl_expiring_during_concurrent_access_is_race_free(app):
    """A key with a very short TTL, hammered by concurrent GETs right as it
    expires, must never raise and must settle to consistently absent."""
    await app.execute(None, [b"SET", b"flicker", b"v", b"PX", b"20"])

    async def hammer():
        for _ in range(200):
            await app.execute(None, [b"GET", b"flicker"])
            await app.execute(None, [b"TTL", b"flicker"])

    await asyncio.gather(*(hammer() for _ in range(10)))
    await asyncio.sleep(0.05)
    assert await app.store.get(b"flicker") is None


async def test_transactions_are_atomic_under_concurrent_load(app):
    """Many concurrent MULTI/EXEC transactions, each doing a read-then-write
    on a shared counter, must not lose updates - which they would if EXEC's
    exclusive gate (server/lifecycle.py) let transactions interleave."""
    await app.execute(None, [b"SET", b"shared", b"0"])

    async def transaction(conn):
        await app.execute(conn, [b"MULTI"])
        await app.execute(conn, [b"INCR", b"shared"])
        reply = await app.execute(conn, [b"EXEC"])
        assert isinstance(reply, Array)

    conns = [_fake_conn() for _ in range(40)]
    await asyncio.gather(*(transaction(c) for c in conns))

    final = await app.execute(None, [b"GET", b"shared"])
    assert final == BulkString(b"40")


async def test_slow_pubsub_subscriber_does_not_block_publisher_or_other_subscribers(app):
    fast_conn = _fake_conn()
    slow_conn = _fake_conn()
    slow_conn.queue = asyncio.Queue(2)  # tiny queue: will fill up and start dropping

    await app.execute(fast_conn, [b"SUBSCRIBE", b"chan"])
    await app.execute(slow_conn, [b"SUBSCRIBE", b"chan"])

    # Publish far more messages than the slow subscriber's queue can hold.
    # publish() must never block on the slow queue - it uses put_nowait and
    # drops on QueueFull (see pubsub/broker.py). The return value is the
    # number of subscribers actually delivered to, so it drops from 2 to 1
    # once the slow subscriber's queue fills - it's never blocked, though.
    delivered_counts = []
    for i in range(50):
        reply = await app.execute(None, [b"PUBLISH", b"chan", f"msg{i}".encode()])
        assert isinstance(reply, Integer)
        delivered_counts.append(reply.value)

    assert delivered_counts[0] == 2  # both subscribers received the first message
    assert delivered_counts[-1] == 1  # the slow one has since been dropped, the fast one hasn't

    # The fast subscriber's queue is effectively unbounded relative to this
    # test (default pubsub_queue_size), so nothing should have been dropped
    # for it; the slow one legitimately lost messages, which is the point.
    assert fast_conn.queue.qsize() <= app.s.pubsub_queue_size
    assert slow_conn.queue.qsize() <= 2


async def test_many_simultaneous_clients_get_independent_state(app):
    """Sanity check that concurrent connections don't share mutable state
    they shouldn't (e.g. MULTI queues bleeding across connections)."""

    async def client(i):
        conn = _fake_conn()
        await app.execute(conn, [b"MULTI"])
        await app.execute(conn, [b"SET", f"k{i}".encode(), str(i).encode()])
        reply = await app.execute(conn, [b"EXEC"])
        assert reply == Array([SimpleString(b"OK")])
        return i

    results = await asyncio.gather(*(client(i) for i in range(100)))
    assert sorted(results) == list(range(100))
    for i in range(100):
        assert await app.store.get(f"k{i}".encode()) == str(i).encode()
