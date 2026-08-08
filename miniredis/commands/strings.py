"""Argument parsing for SET's option flags (EX/PX/PXAT/NX/XX)."""

from __future__ import annotations

import time


def parse_set_options(args: list[bytes]) -> tuple[int | None, bool, bool]:
    """Parse the trailing options of a SET command.

    Returns `(ttl_ms, nx, xx)`, all normalized to a relative TTL in
    milliseconds (PXAT, an absolute unix-ms timestamp, is converted to a
    relative offset from "now"). Raises `SyntaxError` on unknown flags or
    a conflicting `NX`+`XX` combination, matching Redis's own error shape.
    """
    ttl_ms: int | None = None
    nx = xx = False
    i = 0
    while i < len(args):
        option = args[i].upper()
        if option in (b"EX", b"PX", b"PXAT") and i + 1 < len(args):
            raw = int(args[i + 1])
            if option == b"EX":
                ttl_ms = raw * 1000
            elif option == b"PX":
                ttl_ms = raw
            else:  # PXAT: absolute unix-ms timestamp -> relative ms from now
                ttl_ms = max(0, raw - int(time.time() * 1000))
            i += 2
        elif option == b"NX":
            nx = True
            i += 1
        elif option == b"XX":
            xx = True
            i += 1
        else:
            raise SyntaxError

    if nx and xx:
        raise SyntaxError
    return ttl_ms, nx, xx
