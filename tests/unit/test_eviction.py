import pytest

from miniredis.storage import OutOfMemory, Storage


@pytest.mark.asyncio
async def test_noeviction():
    s = Storage(max_memory=120, policy="noeviction")
    with pytest.raises(OutOfMemory):
        await s.set(b"key", b"x" * 100)


@pytest.mark.asyncio
async def test_lru_evicts():
    s = Storage(max_memory=220, policy="allkeys-lru")
    await s.set(b"a", b"x" * 10)
    await s.set(b"b", b"x" * 10)
    await s.get(b"b")
    await s.set(b"c", b"x" * 10)
    assert s.evicted >= 1
