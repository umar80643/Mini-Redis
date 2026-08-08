"""Minimal real-TCP health check for MiniRedis."""

import argparse
import asyncio


async def main(host: str, port: int) -> None:
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(b"*1\r\n$4\r\nPING\r\n")
    await writer.drain()
    response = await reader.read(128)
    writer.close()
    await writer.wait_closed()
    if response != b"+PONG\r\n":
        raise SystemExit(f"unexpected response: {response!r}")
    print("PONG")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()
    asyncio.run(main(args.host, args.port))
