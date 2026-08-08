"""Runtime configuration, loaded from environment variables / a `.env` file.

A single `Settings` object is constructed once at startup and threaded
through the server; nothing reads `os.environ` directly elsewhere, so
every tunable is discoverable in one place and easily overridden in tests.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="", extra="ignore")

    # Networking
    miniredis_host: str = "0.0.0.0"
    miniredis_port: int = Field(6379, ge=0, le=65535)
    max_connections: int = Field(1000, ge=1)
    client_idle_timeout: float = Field(300, gt=0)
    max_request_bytes: int = Field(1_048_576, ge=1024)
    max_bulk_string_bytes: int = Field(1_048_576, ge=1)
    max_array_length: int = Field(1024, ge=1)
    pubsub_queue_size: int = Field(256, ge=1)
    shutdown_timeout: float = Field(10.0, gt=0)

    # Memory management
    max_memory_mb: int = Field(256, ge=1)
    eviction_policy: Literal["noeviction", "allkeys-lru", "allkeys-lfu"] = "allkeys-lru"

    # Durability
    aof_enabled: bool = True
    aof_path: str = "./data/appendonly.aof"
    aof_fsync: Literal["always", "everysec", "no"] = "everysec"
    snapshot_enabled: bool = True
    snapshot_path: str = "./data/dump.json"

    # Replication. `replica_of` is "host:port" of a primary to follow; unset means
    # this node starts as a primary. A node's role can also change at runtime via
    # the REPLICAOF command.
    replica_of: str | None = None
    repl_backlog_bytes: int = Field(1_048_576, ge=1024)
    repl_ack_interval: float = Field(1.0, gt=0)
    repl_reconnect_delay: float = Field(1.0, gt=0)
    repl_read_timeout: float = Field(2.0, gt=0)
    repl_replica_queue_size: int = Field(1000, ge=1)

    # Observability
    log_level: str = "INFO"
    metrics_enabled: bool = True
    metrics_port: int = Field(9121, ge=1, le=65535)
