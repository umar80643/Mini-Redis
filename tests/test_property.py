import pytest

hypothesis = pytest.importorskip("hypothesis", reason="install the dev extra to run property tests")
from hypothesis import given  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from miniredis.protocol import BulkString, encode, parse_one  # noqa: E402


@given(st.binary(max_size=1000))
def test_bulk_roundtrip(x):
    assert parse_one(encode(BulkString(x)))[0] == BulkString(x)
