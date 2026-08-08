"""RESP (REdis Serialization Protocol) value types, encoder, and parser.

This module has no knowledge of sockets or commands: it only converts
between RESP wire bytes and small, typed Python values. The parser is
incremental — it never assumes that one `socket.recv()` call lines up
with exactly one command, since TCP makes no such guarantee. Callers
feed it a growing buffer and it either returns a parsed value plus the
number of bytes consumed, or raises `NeedMoreData` to signal "not yet,
keep reading."
"""

from __future__ import annotations

from dataclasses import dataclass


class RESPError(Exception):
    """Raised when the input bytes violate the RESP wire format."""


class NeedMoreData(Exception):
    """Raised when the buffer holds a partial value; the caller should read more."""


@dataclass(frozen=True)
class SimpleString:
    """RESP simple string, e.g. `+OK\\r\\n`. Used for short, non-binary replies."""

    value: bytes


@dataclass(frozen=True)
class Error:
    """RESP error, e.g. `-ERR wrong number of arguments\\r\\n`."""

    value: bytes


@dataclass(frozen=True)
class Integer:
    """RESP integer, e.g. `:1000\\r\\n`."""

    value: int


@dataclass(frozen=True)
class BulkString:
    """RESP bulk string. `value=None` encodes the RESP null bulk string (`$-1\\r\\n`)."""

    value: bytes | None


@dataclass(frozen=True)
class Array:
    """RESP array of RESP values, e.g. a command's arguments or a MULTI reply."""

    value: list


def encode(message) -> bytes:
    """Serialize a RESP value dataclass into wire bytes."""
    if isinstance(message, SimpleString):
        return b"+" + message.value + b"\r\n"

    if isinstance(message, Error):
        return b"-" + message.value + b"\r\n"

    if isinstance(message, Integer):
        return b":" + str(message.value).encode() + b"\r\n"

    if isinstance(message, BulkString):
        if message.value is None:
            return b"$-1\r\n"
        return b"$" + str(len(message.value)).encode() + b"\r\n" + message.value + b"\r\n"

    if isinstance(message, Array):
        header = b"*" + str(len(message.value)).encode() + b"\r\n"
        body = b"".join(encode(item) for item in message.value)
        return header + body

    raise TypeError(f"cannot encode RESP value of type {type(message)!r}")


def command_bytes(parts) -> bytes:
    """Encode a plain command (list of str/bytes args) as a RESP array of bulk strings.

    Used both to write commands to the AOF and, in tests, to build client requests.
    """
    bulk_items = [
        BulkString(part if isinstance(part, bytes) else str(part).encode()) for part in parts
    ]
    return encode(Array(bulk_items))


def _read_line(buf: bytes, pos: int) -> tuple[bytes, int]:
    """Return the bytes up to the next CRLF and the position just after it."""
    end = buf.find(b"\r\n", pos)
    if end < 0:
        raise NeedMoreData
    return buf[pos:end], end + 2


def parse_one(buf: bytes, pos: int = 0, max_bulk: int = 1_048_576, max_array: int = 1024):
    """Parse a single RESP value starting at `pos`.

    Returns `(value, new_pos)` on success. Raises `NeedMoreData` if the
    buffer doesn't yet contain a complete value, or `RESPError` if the
    bytes present are not valid RESP (or exceed the configured limits).
    """
    if pos >= len(buf):
        raise NeedMoreData

    prefix = buf[pos : pos + 1]
    pos += 1

    if prefix in (b"+", b"-", b":"):
        line, pos = _read_line(buf, pos)
        if prefix == b"+":
            return SimpleString(line), pos
        if prefix == b"-":
            return Error(line), pos
        try:
            return Integer(int(line)), pos
        except ValueError:
            raise RESPError("invalid integer") from None

    if prefix == b"$":
        line, pos = _read_line(buf, pos)
        try:
            length = int(line)
        except ValueError:
            raise RESPError("invalid bulk length") from None
        if length == -1:
            return BulkString(None), pos
        if length < 0 or length > max_bulk:
            raise RESPError("invalid bulk length")
        if len(buf) < pos + length + 2:
            raise NeedMoreData
        if buf[pos + length : pos + length + 2] != b"\r\n":
            raise RESPError("invalid bulk terminator")
        return BulkString(buf[pos : pos + length]), pos + length + 2

    if prefix == b"*":
        line, pos = _read_line(buf, pos)
        try:
            count = int(line)
        except ValueError:
            raise RESPError("invalid array length") from None
        if count < 0 or count > max_array:
            raise RESPError("invalid array length")
        items = []
        for _ in range(count):
            value, pos = parse_one(buf, pos, max_bulk, max_array)
            items.append(value)
        return Array(items), pos

    raise RESPError("unknown RESP prefix")


def parse_command(
    buf: bytes, max_bulk: int = 1_048_576, max_array: int = 1024
) -> tuple[list[bytes], int]:
    """Parse one client command: a RESP array of bulk strings.

    Returns `(argv, bytes_consumed)`. Raises `NeedMoreData` if the buffer
    is incomplete, or `RESPError` if a full but invalid command is present
    (e.g. a non-array frame, non-bulk-string argument, or empty argv).
    """
    value, consumed = parse_one(buf, 0, max_bulk, max_array)
    if not isinstance(value, Array):
        raise RESPError("expected array command")

    argv: list[bytes] = []
    for item in value.value:
        if not isinstance(item, BulkString) or item.value is None:
            raise RESPError("command arguments must be bulk strings")
        argv.append(item.value)

    if not argv:
        raise RESPError("empty command")

    return argv, consumed
