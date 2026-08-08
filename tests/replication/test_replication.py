"""Primary-replica replication tests, covering everything from the assignment:
SET/DELETE/TTL propagation, sequential and concurrent writes, ordering,
disconnect/reconnect, and read-only replica behavior.

Every test here talks to real `MiniRedisServer` instances over real TCP
sockets on ephemeral ports (see `conftest.py`), because the replication
handshake and streaming protocol are exactly what's being verified.
"""

import asyncio

from .conftest import fake_conn, wait_until


async def test_set_replicates_to_a_new_replica_via_full_resync(tmp_path, synced_pair):
    """A key written before the replica even existed must arrive via the
    snapshot sent during the PSYNC full-resync handshake."""
    primary, replica = synced_pair
    await primary.execute(None, [b"SET", b"seeded", b"before-replica-connected"])
    await wait_until(lambda: replica.store.data.get(b"seeded") is not None)
    assert await replica.store.get(b"seeded") == b"before-replica-connected"


async def test_set_replicates_live(synced_pair):
    primary, replica = synced_pair
    await primary.execute(None, [b"SET", b"k", b"v1"])
    await wait_until(lambda: replica.store.data.get(b"k") is not None)
    assert await replica.store.get(b"k") == b"v1"


async def test_delete_replicates(synced_pair):
    primary, replica = synced_pair
    await primary.execute(None, [b"SET", b"k", b"v"])
    await wait_until(lambda: replica.store.data.get(b"k") is not None)

    await primary.execute(None, [b"DEL", b"k"])
    await wait_until(lambda: replica.store.data.get(b"k") is None)
    assert await replica.store.get(b"k") is None


async def test_ttl_and_expiration_replicate(synced_pair):
    """EXPIRE on the primary must set an equivalent TTL on the replica (not a
    fresh, replica-local one — the persisted form is an absolute PXAT
    timestamp precisely so replay/replication don't reset the clock)."""
    primary, replica = synced_pair
    await primary.execute(None, [b"SET", b"k", b"v"])
    await primary.execute(None, [b"EXPIRE", b"k", 100])
    await wait_until(
        lambda: (
            replica.store.data.get(b"k") is not None
            and replica.store.data[b"k"].expires_at is not None
        )
    )

    replica_ttl = await replica.store.ttl(b"k")
    assert 0 < replica_ttl <= 100

    # A key set with a already-past TTL must not appear live on the replica.
    await primary.execute(None, [b"SET", b"expired", b"v", b"PX", b"1"])
    await asyncio.sleep(0.05)
    await primary.execute(None, [b"SET", b"marker", b"done"])  # ordering marker
    await wait_until(lambda: replica.store.data.get(b"marker") is not None)
    assert await replica.store.get(b"expired") is None


async def test_multiple_sequential_writes_replicate_in_order(synced_pair):
    primary, replica = synced_pair
    for i in range(20):
        await primary.execute(None, [b"SET", b"seq", str(i).encode()])

    await wait_until(
        lambda: replica.store.data.get(b"seq") is not None
        and replica.store.data[b"seq"].value == b"19"
    )
    assert await replica.store.get(b"seq") == b"19"


async def test_replication_preserves_write_ordering_via_offset(synced_pair):
    """The replica's applied offset must advance monotonically and end up
    matching the primary's — i.e. nothing was applied out of order or skipped."""
    primary, replica = synced_pair
    for i in range(10):
        await primary.execute(None, [b"SET", f"k{i}".encode(), str(i).encode()])
        await primary.execute(None, [b"DEL", f"k{i}".encode()] if i % 2 == 0 else [b"PING"])

    await wait_until(
        lambda: replica.replication.replica_offset == primary.replication.master_repl_offset
    )
    for i in range(10):
        expected = None if i % 2 == 0 else str(i).encode()
        assert await replica.store.get(f"k{i}".encode()) == expected


async def test_concurrent_writes_all_replicate(synced_pair):
    primary, replica = synced_pair

    async def writer(n):
        await primary.execute(None, [b"SET", f"c{n}".encode(), str(n).encode()])

    await asyncio.gather(*(writer(i) for i in range(50)))
    await wait_until(
        lambda: replica.replication.replica_offset == primary.replication.master_repl_offset
    )

    for i in range(50):
        assert await replica.store.get(f"c{i}".encode()) == str(i).encode()


async def test_replica_rejects_writes_from_ordinary_clients(synced_pair):
    _primary, replica = synced_pair
    reply = await replica.execute(fake_conn(), [b"SET", b"nope", b"x"])
    assert reply.value.startswith(b"READONLY")
    assert await replica.store.get(b"nope") is None


async def test_replica_still_serves_reads(synced_pair):
    primary, replica = synced_pair
    await primary.execute(None, [b"SET", b"readable", b"yes"])
    await wait_until(lambda: replica.store.data.get(b"readable") is not None)
    reply = await replica.execute(fake_conn(), [b"GET", b"readable"])
    assert reply.value == b"yes"


async def test_replica_reconnects_after_link_is_severed_and_catches_up(synced_pair):
    primary, replica = synced_pair
    await primary.execute(None, [b"SET", b"before", b"1"])
    await wait_until(lambda: replica.store.data.get(b"before") is not None)

    # Sever the link from the primary's side, simulating a network blip.
    assert len(primary.replication.replicas) == 1
    for handle in list(primary.replication.replicas.values()):
        handle.conn.writer.close()

    # The primary keeps accepting writes while the replica is disconnected.
    await primary.execute(None, [b"SET", b"during-outage", b"2"])

    await wait_until(
        lambda: replica.replication.link_status == "connected"
        and replica.store.data.get(b"during-outage") is not None,
        timeout=5.0,
    )
    assert await replica.store.get(b"before") == b"1"
    assert await replica.store.get(b"during-outage") == b"2"


async def test_info_reports_consistent_replication_state(synced_pair):
    primary, replica = synced_pair
    await primary.execute(None, [b"SET", b"x", b"1"])
    await wait_until(
        lambda: replica.replication.replica_offset == primary.replication.master_repl_offset
    )

    primary_info = (await primary.execute(None, [b"INFO"])).value.decode()
    replica_info = (await replica.execute(None, [b"INFO"])).value.decode()

    assert "role:master" in primary_info
    assert "connected_slaves:1" in primary_info
    assert "role:slave" in replica_info
    assert "master_link_status:connected" in replica_info
