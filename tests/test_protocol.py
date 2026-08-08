import pytest

from miniredis.protocol import Array, BulkString, NeedMoreData, encode, parse_one


def test_roundtrip():
    value = Array([BulkString(b"GET"), BulkString(b"hello")])
    assert parse_one(encode(value))[0] == value


def test_fragment_need_more():
    with pytest.raises(NeedMoreData):
        parse_one(b"$5\r\nhe")
