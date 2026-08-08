import asyncio

from miniredis.server.connection import ConnectionState


class Writer:
    pass


def test_connection_accounting_state():
    c = ConnectionState(Writer(), peer="local", queue=asyncio.Queue(3))
    c.bytes_read += 10
    c.bytes_written += 20
    c.commands_processed += 1
    assert (c.peer, c.bytes_read, c.bytes_written, c.commands_processed, c.queue.maxsize) == (
        "local",
        10,
        20,
        1,
        3,
    )
