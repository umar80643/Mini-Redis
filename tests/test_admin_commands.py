"""Regression tests for INFO, SAVE, and COMMAND.

These three commands had no test coverage before this suite was added,
which is exactly why a broken `INFO` (calling `build_info(self)` inside a
plain function with no `self` in scope) and a broken `SAVE` (referencing
an undefined module-level `log`) both shipped without failing CI. Every
admin/introspection command should be exercised at least once.
"""

import pytest

from miniredis.config import Settings
from miniredis.protocol import Array, BulkString, SimpleString
from miniredis.server import MiniRedisServer


@pytest.fixture
async def app(tmp_path):
    settings = Settings(
        aof_enabled=False,
        snapshot_enabled=False,
        snapshot_path=str(tmp_path / "dump.json"),
    )
    server = MiniRedisServer(settings)
    yield server


async def test_info_reports_server_and_keyspace_sections(app):
    await app.execute(None, [b"SET", b"a", b"1"])
    await app.execute(None, [b"SET", b"b", b"2"])

    reply = await app.execute(None, [b"INFO"])

    assert isinstance(reply, BulkString)
    body = reply.value.decode()
    assert "# Server" in body
    assert "# Memory" in body
    assert "# Keyspace" in body
    assert "db0:keys=2" in body


async def test_save_writes_snapshot_and_truncates_aof(tmp_path):
    settings = Settings(
        aof_enabled=True,
        aof_path=str(tmp_path / "appendonly.aof"),
        snapshot_enabled=True,
        snapshot_path=str(tmp_path / "dump.json"),
    )
    server = MiniRedisServer(settings)
    await server.start()
    try:
        await server.execute(None, [b"SET", b"k", b"v"])
        reply = await server.execute(None, [b"SAVE"])

        assert isinstance(reply, SimpleString)
        assert reply.value == b"OK"
        assert (tmp_path / "dump.json").exists()
        # SAVE resets the AOF once its contents are captured in the snapshot.
        assert (tmp_path / "appendonly.aof").read_bytes() == b""
    finally:
        await server.close()


async def test_command_lists_registered_commands_with_arity(app):
    reply = await app.execute(None, [b"COMMAND"])

    assert isinstance(reply, Array)
    names = {row.value[0].value.decode() for row in reply.value}
    assert {"GET", "SET", "INFO", "SAVE", "EXPIRE"} <= names
