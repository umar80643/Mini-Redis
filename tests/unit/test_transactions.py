"""MULTI/EXEC/DISCARD coverage.

Previously untested despite being the one command family with real
concurrency implications: EXEC runs under `ExecutionGate.enter_exclusive`
so a transaction's queued commands can't interleave with another
connection's commands mid-execution (see server/lifecycle.py). These
tests exercise the transaction state machine directly through
`MiniRedisServer.execute`, the same entry point the TCP layer uses.
"""

import pytest

from miniredis.config import Settings
from miniredis.protocol import Array, BulkString, Error, Integer, SimpleString
from miniredis.server import Conn, MiniRedisServer


def _new_conn():
    class _FakeWriter:
        def write(self, _data):
            pass

        async def drain(self):
            pass

    return Conn(_FakeWriter())


@pytest.fixture
def app(tmp_path):
    settings = Settings(
        aof_enabled=False, snapshot_enabled=False, snapshot_path=str(tmp_path / "x")
    )
    return MiniRedisServer(settings)


async def test_commands_queue_during_multi_and_run_on_exec(app):
    conn = _new_conn()

    assert await app.execute(conn, [b"MULTI"]) == SimpleString(b"OK")
    assert await app.execute(conn, [b"SET", b"a", b"1"]) == SimpleString(b"QUEUED")
    assert await app.execute(conn, [b"INCR", b"a"]) == SimpleString(b"QUEUED")
    assert conn.multi is True
    # Nothing actually applied yet - still queued.
    assert await app.execute(conn, [b"GET", b"a"]) == SimpleString(b"QUEUED")

    reply = await app.execute(conn, [b"EXEC"])
    assert isinstance(reply, Array)
    assert reply.value == [SimpleString(b"OK"), Integer(2), BulkString(b"2")]
    assert conn.multi is False


async def test_discard_drops_queued_commands_without_running_them(app):
    conn = _new_conn()

    await app.execute(conn, [b"MULTI"])
    await app.execute(conn, [b"SET", b"a", b"1"])
    assert await app.execute(conn, [b"DISCARD"]) == SimpleString(b"OK")

    # The queued SET must never have run.
    assert await app.execute(conn, [b"GET", b"a"]) == BulkString(None)
    assert conn.multi is False


async def test_exec_without_multi_is_an_error(app):
    conn = _new_conn()
    reply = await app.execute(conn, [b"EXEC"])
    assert isinstance(reply, Error)
    assert b"EXEC without MULTI" in reply.value


async def test_multi_cannot_be_nested(app):
    conn = _new_conn()
    await app.execute(conn, [b"MULTI"])
    reply = await app.execute(conn, [b"MULTI"])
    assert isinstance(reply, Error)
    assert b"nested" in reply.value
