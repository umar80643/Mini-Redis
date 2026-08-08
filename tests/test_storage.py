import pytest

from miniredis.storage import Storage


@pytest.mark.asyncio
async def test_storage_and_counter():
    s = Storage()
    assert await s.set(b"a", b"1")
    assert await s.get(b"a") == b"1"
    assert await s.incr(b"a", 2) == 3
    assert await s.delete(b"a") == 1


@pytest.mark.asyncio
async def test_ttl():
    s = Storage()
    await s.set(b"a", b"x", 1000)
    assert await s.ttl(b"a", True) <= 1000
    assert await s.persist(b"a") == 1
    assert await s.ttl(b"a") == -1
