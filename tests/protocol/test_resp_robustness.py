import pytest

from miniredis.protocol import (
    NeedMoreData,
    RESPError,
    command_bytes,
    parse_command,
    parse_one,
)


def test_command_one_byte_fragmentation():
    wire = command_bytes([b"SET", b"bin", b"\x00\xffvalue"])
    for n in range(1, len(wire)):
        with pytest.raises(NeedMoreData):
            parse_command(wire[:n])
    parts, used = parse_command(wire)
    assert parts == [b"SET", b"bin", b"\x00\xffvalue"]
    assert used == len(wire)


def test_multiple_commands_buffer():
    a = command_bytes([b"PING"])
    b = command_bytes([b"GET", b"x"])
    p, n = parse_command(a + b)
    assert p == [b"PING"]
    p2, n2 = parse_command((a + b)[n:])
    assert p2 == [b"GET", b"x"]
    assert n + n2 == len(a + b)


def test_reject_bad_lengths_and_terminator():
    with pytest.raises(RESPError):
        parse_one(b"$-2\r\n")
    with pytest.raises(RESPError):
        parse_one(b"$3\r\nabcXX")
    with pytest.raises(RESPError):
        parse_one(b"*9999\r\n", max_array=10)
