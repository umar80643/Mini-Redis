"""Coordinates ordinary command execution with exclusive MULTI/EXEC transactions."""

from __future__ import annotations

import asyncio


class ExecutionGate:
    """A readers-writer lock, repurposed for command concurrency.

    Ordinary commands run as "readers" and may run concurrently with each
    other. An EXEC transaction runs as the sole "writer": it waits for all
    in-flight ordinary commands to finish, then blocks new ones from
    starting, so a transaction's queued commands can't interleave with
    commands from other connections mid-execution.
    """

    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._readers = 0
        self._writer_active = False

    async def enter_shared(self) -> None:
        async with self._condition:
            while self._writer_active:
                await self._condition.wait()
            self._readers += 1

    async def exit_shared(self) -> None:
        async with self._condition:
            self._readers -= 1
            if self._readers == 0:
                self._condition.notify_all()

    async def enter_exclusive(self) -> None:
        async with self._condition:
            while self._writer_active or self._readers:
                await self._condition.wait()
            self._writer_active = True

    async def exit_exclusive(self) -> None:
        async with self._condition:
            self._writer_active = False
            self._condition.notify_all()
