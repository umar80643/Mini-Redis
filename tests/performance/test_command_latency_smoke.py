"""Non-gating performance smoke tests; real numbers come from benchmarks/."""

import pytest

from miniredis.storage import Storage


@pytest.mark.asyncio
async def test_repeated_storage_operations_remain_correct():
    store = Storage()
    for i in range(1000):
        key = f"k:{i}".encode()
        assert await store.set(key, b"v")
        assert await store.get(key) == b"v"
    assert len(await store.keys()) == 1000
