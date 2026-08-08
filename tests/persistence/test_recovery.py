import pytest

from miniredis.persistence import AOF, load_snapshot, save_snapshot
from miniredis.storage import Storage


@pytest.mark.asyncio
async def test_aof_truncated_tail_and_corruption(tmp_path):
    p = tmp_path / "aof"
    a = AOF(str(p), "always", True)
    await a.open()
    await a.append([b"SET", b"a", b"1"])
    await a.close()
    p.write_bytes(p.read_bytes() + b"*2\r\n$3\r\nSET\r\n$1\r\n")
    assert AOF(str(p), "always", True).read_commands() == [[b"SET", b"a", b"1"]]


@pytest.mark.asyncio
async def test_snapshot_preserves_ttl(tmp_path):
    p = tmp_path / "dump.json"
    s = Storage()
    await s.set(b"a", b"v", 5000)
    await save_snapshot(s, str(p))
    s2 = Storage()
    assert await load_snapshot(s2, str(p)) == 1
    assert await s2.get(b"a") == b"v"
    assert 0 < await s2.ttl(b"a", True) <= 5000


@pytest.mark.asyncio
async def test_aof_absolute_ttl_counts_downtime(tmp_path):
    import time

    from miniredis.config import Settings
    from miniredis.server import MiniRedisServer

    app = MiniRedisServer(Settings(aof_enabled=False, snapshot_enabled=False))
    past = str(int(time.time() * 1000) - 1000).encode()
    await app.execute(None, [b"SET", b"expired", b"value", b"PXAT", past], replay=True)
    assert await app.store.get(b"expired") is None
