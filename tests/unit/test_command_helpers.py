import pytest

from miniredis.commands.expiration import expiration_ms
from miniredis.commands.generic import parse_scan_options
from miniredis.commands.registry import COMMANDS
from miniredis.commands.strings import parse_set_options


def test_set_option_validation():
    ttl, nx, xx = parse_set_options([b"EX", b"2", b"NX"])
    assert ttl == 2000 and nx and not xx
    with pytest.raises(SyntaxError):
        parse_set_options([b"NX", b"XX"])


def test_scan_options():
    assert parse_scan_options([b"MATCH", b"user:*", b"COUNT", b"5"]) == (5, b"user:*")
    with pytest.raises(ValueError):
        parse_scan_options([b"COUNT", b"0"])


def test_registry_metadata_and_expiration():
    spec = COMMANDS["SET"]
    assert spec.write and spec.min_args <= 2 and (spec.max_args is None or 2 <= spec.max_args)
    assert expiration_ms("EXPIRE", 2) == 2000
