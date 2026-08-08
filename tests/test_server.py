import pytest

from miniredis.config import Settings
from miniredis.protocol import BulkString, Integer, SimpleString
from miniredis.server import MiniRedisServer


@pytest.mark.asyncio
async def test_commands(tmp_path):
    s = Settings(aof_enabled=False, snapshot_enabled=False, snapshot_path=str(tmp_path / "x"))
    app = MiniRedisServer(s)
    assert isinstance(await app.execute(None, [b"PING"]), SimpleString)
    await app.execute(None, [b"SET", b"x", b"10"])
    r = await app.execute(None, [b"INCR", b"x"])
    assert isinstance(r, Integer) and r.value == 11
    r = await app.execute(None, [b"GET", b"x"])
    assert isinstance(r, BulkString) and r.value == b"11"
