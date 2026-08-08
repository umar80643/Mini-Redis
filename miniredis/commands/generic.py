"""Argument parsing for SCAN's option flags (MATCH/COUNT)."""

from __future__ import annotations


def parse_scan_options(args: list[bytes]) -> tuple[int, bytes]:
    """Parse SCAN's optional `MATCH pattern` / `COUNT n` flags.

    Returns `(count, pattern)` with defaults of `(10, b"*")`. Raises
    `SyntaxError` on an unrecognized flag and `ValueError` if COUNT isn't
    a positive integer.
    """
    count = 10
    pattern = b"*"
    i = 0
    while i < len(args):
        flag = args[i].upper()
        if flag == b"COUNT" and i + 1 < len(args):
            count = int(args[i + 1])
            i += 2
        elif flag == b"MATCH" and i + 1 < len(args):
            pattern = args[i + 1]
            i += 2
        else:
            raise SyntaxError

    if count <= 0:
        raise ValueError("COUNT must be positive")
    return count, pattern
