import asyncio
import socket

import pytest

from miniredis.config import Settings
from miniredis.protocol import BulkString, SimpleString, command_bytes, parse_one
from miniredis.server import MiniRedisServer


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def request(port, parts):
    r, w = await asyncio.open_connection("127.0.0.1", port)
    w.write(command_bytes(parts))
    await w.drain()
    data = await r.read(65536)
    w.close()
    await w.wait_closed()
    return parse_one(data)[0]


@pytest.mark.asyncio
async def test_real_tcp_and_fragmentation(tmp_path):
    port = free_port()
    app = MiniRedisServer(
        Settings(
            miniredis_host="127.0.0.1",
            miniredis_port=port,
            metrics_enabled=False,
            aof_enabled=False,
            snapshot_enabled=False,
            snapshot_path=str(tmp_path / "x"),
        )
    )
    await app.start()
    try:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        wire = command_bytes([b"SET", b"k", b"v"])
        for b in wire:
            w.write(bytes([b]))
            await w.drain()
        assert isinstance(parse_one(await r.read(128))[0], SimpleString)
        w.write(command_bytes([b"GET", b"k"]))
        await w.drain()
        x = parse_one(await r.read(128))[0]
        assert isinstance(x, BulkString) and x.value == b"v"
        w.close()
        await w.wait_closed()
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_concurrent_counter_10000(tmp_path):
    port = free_port()
    app = MiniRedisServer(
        Settings(
            miniredis_host="127.0.0.1",
            miniredis_port=port,
            metrics_enabled=False,
            aof_enabled=False,
            snapshot_enabled=False,
            snapshot_path=str(tmp_path / "x"),
        )
    )
    await app.start()

    async def worker():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        for _ in range(100):
            w.write(command_bytes([b"INCR", b"counter"]))
            await w.drain()
            await r.readuntil(b"\r\n")
        w.close()
        await w.wait_closed()

    try:
        await asyncio.gather(*(worker() for _ in range(100)))
        res = await request(port, [b"GET", b"counter"])
        assert isinstance(res, BulkString) and res.value == b"10000"
    finally:
        await app.close()


@pytest.mark.asyncio
async def test_client_disconnect_during_response_does_not_crash_server(tmp_path):
    import socket

    from miniredis.config import Settings
    from miniredis.server import MiniRedisServer

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    settings = Settings(
        miniredis_host="127.0.0.1",
        miniredis_port=port,
        metrics_enabled=False,
        aof_enabled=False,
        snapshot_enabled=False,
        aof_path=str(tmp_path / "aof"),
        snapshot_path=str(tmp_path / "snap"),
    )
    server = MiniRedisServer(settings)
    await server.start()
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
        await writer.drain()
        writer.transport.abort()
        await asyncio.sleep(0.05)

        reader2, writer2 = await asyncio.open_connection("127.0.0.1", port)
        writer2.write(b"*1\r\n$4\r\nPING\r\n")
        await writer2.drain()
        assert await reader2.readuntil(b"\r\n") == b"+PONG\r\n"
        writer2.close()
        await writer2.wait_closed()
    finally:
        await asyncio.wait_for(server.close(), 3.0)


@pytest.mark.asyncio
async def test_shutdown_with_active_client_closes_connection(tmp_path):
    import socket

    from miniredis.config import Settings
    from miniredis.server import MiniRedisServer

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    settings = Settings(
        miniredis_host="127.0.0.1",
        miniredis_port=port,
        metrics_enabled=False,
        aof_enabled=False,
        snapshot_enabled=False,
        shutdown_timeout=1.0,
        aof_path=str(tmp_path / "aof"),
        snapshot_path=str(tmp_path / "snap"),
    )
    server = MiniRedisServer(settings)
    await server.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.sleep(0.02)
    await asyncio.wait_for(server.close(), 3.0)
    assert server.ready is False
    assert await asyncio.wait_for(reader.read(), 1.0) == b""
    writer.close()
