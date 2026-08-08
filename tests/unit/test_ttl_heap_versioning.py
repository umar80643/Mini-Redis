"""Proves out the versioned-TTL-heap design (see storage/expiration.py's module docstring).

A naive `heapq` keyed only on expiry time would leak a stale record every
time a key's TTL changes or the key is overwritten, and — worse — would
wrongly expire a key that no longer has anything to do with that record.
These tests set up exactly that scenario and confirm the version check
in `Storage.expire_cycle` catches it.
"""

import pytest

from miniredis.storage import Storage


@pytest.mark.asyncio
async def test_stale_heap_record_does_not_expire_a_renewed_key():
    """Shortening then extending a key's TTL must not let the old, shorter
    deadline delete a key that was given a longer one afterwards."""
    store = Storage()
    await store.set(b"k", b"v", ttl_ms=10)  # schedules an early heap record (version 1)
    await store.expire(b"k", 1_000_000)  # bumps version to 2, schedules a far-future record

    # The version-1 heap record is still sitting in the heap, "due" almost immediately,
    # but it must not be allowed to delete the still-live, far-future key.
    removed = await store.expire_cycle()
    assert removed == 0
    assert await store.get(b"k") == b"v"


@pytest.mark.asyncio
async def test_stale_heap_record_does_not_expire_an_overwritten_key():
    """Overwriting a short-TTL key with a permanent SET must not let the
    old heap record delete the new, TTL-less value once its old deadline passes."""
    store = Storage()
    await store.set(b"k", b"v1", ttl_ms=10)  # schedules a heap record (version 1)
    await store.set(b"k", b"v2")  # overwrites with no TTL; version becomes 2, no new heap record

    removed = await store.expire_cycle()
    assert removed == 0
    assert await store.get(b"k") == b"v2"


@pytest.mark.asyncio
async def test_expire_cycle_still_removes_genuinely_expired_keys():
    """Sanity check: the version guard must not suppress real expirations."""
    store = Storage()
    await store.set(b"k", b"v", ttl_ms=-1)  # already in the past

    removed = await store.expire_cycle()
    assert removed == 1
    assert await store.get(b"k") is None
