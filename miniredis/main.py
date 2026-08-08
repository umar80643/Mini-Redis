"""Process entry point: wires up config, logging, and graceful shutdown on signals."""

from __future__ import annotations

import asyncio
import signal

from .config import Settings
from .observability.logging import configure
from .server import MiniRedisServer


async def run() -> None:
    settings = Settings()
    configure(settings.log_level)

    server = MiniRedisServer(settings)
    await server.start()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # e.g. Windows, where add_signal_handler isn't supported
            continue

    await stop_event.wait()
    await server.close()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
