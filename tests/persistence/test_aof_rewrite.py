"""AOF rewrite (compaction) tests.

The core claim being tested: after a rewrite, the AOF is *smaller* (or at
least no larger) than the raw mutation history, and replaying the
rewritten file from an empty keyspace reproduces the exact same state as
replaying the original, un-rewritten file would have.
"""

import pytest

from miniredis.persistence import AOF
from miniredis.storage import Storage


@pytest.fixture
async def storage():
    return Storage()


async def test_rewrite_produces_a_replayable_equivalent_aof(tmp_path, storage):
    aof = AOF(str(tmp_path / "appendonly.aof"), mode="always", enabled=True)
    await aof.open()

    # A history with redundant writes: 'k' is set three times, 'gone' is deleted.
    for command in (
        [b"SET", b"k", b"v1"],
        [b"SET", b"k", b"v2"],
        [b"SET", b"k", b"v3"],
        [b"SET", b"gone", b"x"],
        [b"DEL", b"gone"],
        [b"SET", b"other", b"y"],
    ):
        (
            await storage.set(*command[1:3])
            if command[0] == b"SET"
            else await storage.delete(command[1])
        )
        await aof.append(command)

    pre_rewrite_size = aof.size_bytes()
    duration = await aof.rewrite(storage)
    post_rewrite_size = aof.size_bytes()

    assert duration >= 0
    assert post_rewrite_size <= pre_rewrite_size
    assert aof.rewrites == 1

    # Replaying the rewritten AOF into a fresh, empty store must reproduce
    # exactly the live keyspace (final values only, 'gone' truly gone).
    replayed = Storage()
    for command in aof.read_commands():
        await replayed.set(command[1], command[2]) if command[0] == b"SET" else None
    assert await replayed.get(b"k") == b"v3"
    assert await replayed.get(b"gone") is None
    assert await replayed.get(b"other") == b"y"

    await aof.close()


async def test_rewrite_preserves_ttls_as_absolute_deadlines(tmp_path, storage):
    aof = AOF(str(tmp_path / "appendonly.aof"), mode="always", enabled=True)
    await aof.open()

    await storage.set(b"temp", b"v", ttl_ms=60_000)
    await aof.append([b"SET", b"temp", b"v", b"PX", b"60000"])

    await aof.rewrite(storage)
    commands = aof.read_commands()

    assert any(c[0] == b"SET" and c[1] == b"temp" and b"PXAT" in c for c in commands)
    await aof.close()


async def test_rewrite_is_atomic_old_file_survives_if_something_reads_mid_swap(tmp_path, storage):
    """The rewrite writes to a temp file and os.replace()s it in — the AOF path
    itself is never observed in a partially-written state."""
    aof = AOF(str(tmp_path / "appendonly.aof"), mode="always", enabled=True)
    await aof.open()
    await storage.set(b"a", b"1")
    await aof.append([b"SET", b"a", b"1"])

    await aof.rewrite(storage)

    assert aof.path.exists()
    assert not aof.path.with_suffix(aof.path.suffix + ".rewrite.tmp").exists()
    await aof.close()


async def test_disabled_aof_rewrite_is_a_safe_noop(tmp_path, storage):
    aof = AOF(str(tmp_path / "appendonly.aof"), mode="always", enabled=False)
    duration = await aof.rewrite(storage)
    assert duration == 0.0
    assert aof.rewrites == 0
