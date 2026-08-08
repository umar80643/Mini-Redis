"""A minimal async RESP client, used only for benchmarking - not a general-purpose
client library. Deliberately reuses the project's own protocol encoder/parser
rather than a third-party Redis client, so the benchmark measures MiniRedis's
actual wire format end to end.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from miniredis.protocol import NeedMoreData, command_bytes, parse_one  # noqa: E402


class BenchClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 6379):
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._buf = b""

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def close(self) -> None:
        if self.writer is None:
            return
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except (ConnectionError, OSError):
            pass

    async def _read_one(self):
        while True:
            try:
                value, consumed = parse_one(self._buf)
                self._buf = self._buf[consumed:]
                return value
            except NeedMoreData:
                chunk = await self.reader.read(65536)
                if not chunk:
                    raise ConnectionError("server closed connection during benchmark") from None
                self._buf += chunk

    async def request(self, parts: list[bytes]):
        """Send one command and wait for its reply."""
        self.writer.write(command_bytes(parts))
        await self.writer.drain()
        return await self._read_one()

    async def pipeline(self, commands: list[list[bytes]]) -> list:
        """Send many commands back-to-back before reading any replies."""
        self.writer.write(b"".join(command_bytes(c) for c in commands))
        await self.writer.drain()
        return [await self._read_one() for _ in commands]
