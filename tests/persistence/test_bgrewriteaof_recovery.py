"""End-to-end AOF rewrite: the BGREWRITEAOF command, and recovering correctly
from a rewritten (compacted) AOF file on a fresh server startup."""

from miniredis.config import Settings
from miniredis.protocol import SimpleString
from miniredis.server import MiniRedisServer


async def test_bgrewriteaof_compacts_history(tmp_path):
    settings = Settings(
        aof_enabled=True,
        aof_path=str(tmp_path / "appendonly.aof"),
        snapshot_enabled=False,
        snapshot_path=str(tmp_path / "dump.json"),
        metrics_enabled=False,
        miniredis_port=0,
    )
    server = MiniRedisServer(settings)
    await server.start()
    try:
        # Redundant history: the same key overwritten many times, one deleted.
        for i in range(10):
            await server.execute(None, [b"SET", b"hot", str(i).encode()])
        await server.execute(None, [b"SET", b"gone", b"x"])
        await server.execute(None, [b"DEL", b"gone"])
        await server.execute(None, [b"SET", b"stable", b"y"])

        size_before = server.aof.size_bytes()
        reply = await server.execute(None, [b"BGREWRITEAOF"])

        assert reply == SimpleString(b"OK")
        assert server.aof.rewrites == 1
        assert server.aof.size_bytes() <= size_before
    finally:
        # Skip the extra snapshot+reset that close() would normally also do -
        # we want the rewritten file on disk exactly as a crash would leave
        # it, to prove recovery from a *rewritten* AOF specifically.
        if server.exp_task:
            server.exp_task.cancel()
        if server.replication.link_task:
            server.replication.link_task.cancel()
        await server.aof.close()


async def test_recovering_from_a_rewritten_aof_reproduces_compacted_state(tmp_path):
    settings = Settings(
        aof_enabled=True,
        aof_path=str(tmp_path / "appendonly.aof"),
        snapshot_enabled=False,
        snapshot_path=str(tmp_path / "dump.json"),
        metrics_enabled=False,
        miniredis_port=0,
    )
    server = MiniRedisServer(settings)
    await server.start()
    try:
        for i in range(10):
            await server.execute(None, [b"SET", b"hot", str(i).encode()])
        await server.execute(None, [b"SET", b"gone", b"x"])
        await server.execute(None, [b"DEL", b"gone"])
        await server.execute(None, [b"SET", b"stable", b"y"])
        await server.execute(None, [b"BGREWRITEAOF"])
    finally:
        if server.exp_task:
            server.exp_task.cancel()
        if server.replication.link_task:
            server.replication.link_task.cancel()
        await server.aof.close()

    recovered = MiniRedisServer(settings)
    await recovered.start()
    try:
        assert await recovered.store.get(b"hot") == b"9"
        assert await recovered.store.get(b"gone") is None
        assert await recovered.store.get(b"stable") == b"y"
    finally:
        await recovered.close()
