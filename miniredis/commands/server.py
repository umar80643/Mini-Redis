"""Builds the human-readable payload for the INFO command."""

from __future__ import annotations

import time

from ..replication.master import replication_info


def build_info(server) -> bytes:
    """Render an `INFO`-style report of server, memory, keyspace, and replication stats.

    Mirrors the section/`key:value` layout real Redis uses so existing
    RESP client libraries and monitoring scrapers parse it without changes.
    """
    store = server.store
    keys_with_ttl = sum(1 for entry in store.data.values() if entry.expires_at is not None)
    uptime_seconds = int(time.monotonic() - server.started)
    hits = server.get_hits
    misses = server.get_misses
    total_gets = hits + misses
    hit_ratio = (hits / total_gets) if total_gets else 0.0

    sections = [
        "# Server",
        "miniredis_version:1.0.0",
        f"uptime_in_seconds:{uptime_seconds}",
        "# Clients",
        f"connected_clients:{len(server.clients)}",
        f"total_connections_received:{server.total_connections}",
        "# Memory",
        f"used_memory:{store.used}",
        f"used_memory_peak:{store.peak}",
        f"maxmemory:{store.max_memory}",
        f"maxmemory_policy:{store.policy}",
        "# Stats",
        f"total_commands_processed:{server.total_commands}",
        f"expired_keys:{store.expired}",
        f"evicted_keys:{store.evicted}",
        f"protocol_errors:{server.protocol_errors}",
        f"keyspace_hits:{hits}",
        f"keyspace_misses:{misses}",
        f"keyspace_hit_ratio:{hit_ratio:.4f}",
        "# Persistence",
        f"aof_enabled:{int(server.aof.enabled)}",
        f"aof_size_bytes:{server.aof.size_bytes()}",
        f"aof_writes:{server.aof.writes}",
        f"aof_rewrites:{server.aof.rewrites}",
        *replication_info(server),
        "# Keyspace",
        f"db0:keys={len(store.data)},expires={keys_with_ttl}",
    ]
    return ("\r\n".join(sections) + "\r\n").encode()
