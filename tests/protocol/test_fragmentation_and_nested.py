import pytest

from miniredis.protocol import Array, BulkString, NeedMoreData, encode, parse_one


def test_nested_array_round_trip():
    value = Array([BulkString(b"a"), Array([BulkString(b"b")])])
    parsed, used = parse_one(encode(value))
    assert parsed == value
    assert used == len(encode(value))


def test_every_prefix_of_command_is_incomplete_until_complete():
    payload = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
    for i in range(len(payload)):
        with pytest.raises(NeedMoreData):
            parse_one(payload[:i])
