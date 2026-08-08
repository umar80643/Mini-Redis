import asyncio

import pytest

from miniredis.pubsub import Broker
from miniredis.server.connection import ConnectionState
from miniredis.storage import OutOfMemory, Storage


class Writer:
    pass


@pytest.mark.asyncio
async def test_slow_subscriber_queue_is_bounded_and_does_not_block_publish():
    b = Broker()
    c = ConnectionState(Writer(), queue=asyncio.Queue(1))
    b.subscribe(c, b"orders")
    assert await b.publish(b"orders", b"one") == 1
    assert await b.publish(b"orders", b"two") == 0
    assert c.queue.qsize() == 1


@pytest.mark.asyncio
async def test_noeviction_memory_limit_fails_predictably():
    s = Storage(max_memory=110, policy="noeviction")
    with pytest.raises(OutOfMemory):
        await s.set(b"key", b"x" * 100)


@pytest.mark.asyncio
async def test_snapshot_write_failure_preserves_previous_snapshot(tmp_path, monkeypatch):
    import miniredis.persistence.core as persistence_core
    from miniredis.persistence import save_snapshot

    storage = Storage()
    await storage.set(b"stable", b"value")
    target = tmp_path / "dump.json"
    target.write_text('{"previous":true}')

    real_replace = persistence_core.os.replace

    def fail_replace(src, dst):
        raise OSError("simulated disk/rename failure")

    monkeypatch.setattr(persistence_core.os, "replace", fail_replace)

    with pytest.raises(OSError, match="simulated"):
        await save_snapshot(storage, str(target))

    assert target.read_text() == '{"previous":true}'
    assert not (tmp_path / "dump.json.tmp").exists()
    monkeypatch.setattr(persistence_core.os, "replace", real_replace)


@pytest.mark.asyncio
async def test_aof_disk_write_failure_is_not_silently_swallowed(tmp_path):
    from miniredis.persistence import AOF

    class FailingFile:
        def write(self, data):
            raise OSError("disk full")

    aof = AOF(str(tmp_path / "appendonly.aof"), mode="no", enabled=True)
    aof.fp = FailingFile()
    with pytest.raises(OSError, match="disk full"):
        await aof.append([b"SET", b"key", b"value"])
