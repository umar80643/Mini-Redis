"""Prometheus metric definitions. One process-wide registry, imported wherever a
command handler or background task needs to record something.
"""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

COMMANDS = Counter("miniredis_commands_total", "Commands", ["command", "status"])
LATENCY = Histogram("miniredis_command_duration_seconds", "Command latency", ["command"])
CLIENTS = Gauge("miniredis_connected_clients", "Connected clients")
CONNECTIONS = Counter("miniredis_connections_total", "Connections accepted")
KEYS = Gauge("miniredis_keys", "Live keys")
MEMORY = Gauge("miniredis_memory_bytes", "Approximate memory")
PEAK_MEMORY = Gauge("miniredis_peak_memory_bytes", "Peak approximate memory")
EXPIRED = Counter("miniredis_expired_keys_total", "Expired keys")
EVICTED = Counter("miniredis_evicted_keys_total", "Evicted keys")
PROTOCOL_ERRORS = Counter("miniredis_protocol_errors_total", "Protocol errors")
AOF_WRITES = Counter("miniredis_aof_writes_total", "AOF writes")
PUBSUB_MESSAGES = Counter("miniredis_pubsub_messages_total", "Published messages")

# Cache effectiveness
GET_HITS = Counter("miniredis_get_hits_total", "GET calls that found a live key")
GET_MISSES = Counter("miniredis_get_misses_total", "GET calls for an absent/expired key")

# AOF rewrite / compaction
AOF_REWRITES = Counter("miniredis_aof_rewrites_total", "Completed AOF rewrite/compaction runs")
AOF_REWRITE_DURATION = Histogram("miniredis_aof_rewrite_duration_seconds", "AOF rewrite duration")
AOF_SIZE_BYTES = Gauge("miniredis_aof_size_bytes", "Current AOF file size on disk")

# Replication
REPL_MASTER_OFFSET = Gauge("miniredis_repl_master_offset_bytes", "This node's replication offset")
REPL_CONNECTED_REPLICAS = Gauge(
    "miniredis_repl_connected_replicas", "Replicas currently attached (primary only)"
)
REPL_REPLICA_LAG = Gauge(
    "miniredis_repl_replica_lag_bytes",
    "Bytes this node's link is behind its primary (replica only)",
)


def start(port: int) -> None:
    start_http_server(port)
