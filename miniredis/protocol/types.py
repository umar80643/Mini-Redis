"""Type alias for "any RESP reply value", used in handler/dispatcher signatures."""

from __future__ import annotations

from .core import Array, BulkString, Error, Integer, SimpleString

RESPValue = SimpleString | BulkString | Error | Integer | Array

__all__ = ["RESPValue", "Array", "BulkString", "Error", "Integer", "SimpleString"]
