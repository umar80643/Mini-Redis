import asyncio
import logging

log = logging.getLogger("miniredis")


class Broker:
    """In-process Pub/Sub broker with bounded per-connection queues.

    Publishing is deliberately non-blocking: a slow subscriber whose queue is
    full drops the new message instead of applying unbounded backpressure to
    publishers or consuming unlimited memory.
    """

    def __init__(self):
        self.channels = {}
        self.by_conn = {}

    def subscribe(self, connection, channel):
        self.channels.setdefault(channel, set()).add(connection)
        self.by_conn.setdefault(connection, set()).add(channel)

    def unsubscribe(self, connection, channel=None):
        targets = list(self.by_conn.get(connection, set())) if channel is None else [channel]
        for target in targets:
            self.channels.get(target, set()).discard(connection)
            if not self.channels.get(target):
                self.channels.pop(target, None)
            self.by_conn.get(connection, set()).discard(target)
        if not self.by_conn.get(connection):
            self.by_conn.pop(connection, None)

    async def publish(self, channel, message):
        sent = 0
        for connection in list(self.channels.get(channel, set())):
            try:
                connection.queue.put_nowait((channel, message))
                sent += 1
            except asyncio.QueueFull:
                log.warning(
                    "pubsub_message_dropped",
                    extra={"event": "pubsub_message_dropped", "connection_id": connection.id},
                )
        return sent
