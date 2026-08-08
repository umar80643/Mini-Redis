# Master Prompt Compliance Audit

This repository was re-audited against the 64-section MiniRedis master build prompt on 2026-08-05.

## Implemented core
- Async TCP server using `asyncio.start_server`, incremental RESP parser, binary-safe bulk strings, fragmentation/multiple-command handling and protocol limits.
- Connection state with ID, peer, transaction state, bounded Pub/Sub queue, bytes read/written and per-connection command count.
- Command registry metadata plus dispatcher separated from TCP networking. Domain helpers are split across strings, expiration, generic, transactions, Pub/Sub and server modules.
- In-memory storage abstraction with explicit Entry model, approximate memory accounting, LRU/LFU/noeviction victim selection, lazy TTL and versioned min-heap active expiration.
- PING/ECHO, SET/GET/GETDEL/MGET/MSET, DEL/EXISTS/TYPE, counters, TTL commands, DBSIZE/FLUSHDB, KEYS/SCAN, INFO/COMMAND, SAVE, transactions and Pub/Sub.
- AOF (`always`, `everysec`, `no`), RESP replay, truncated-tail recovery, corruption detection, atomic JSON snapshot and absolute-expiry recovery.
- Structured JSON logs, Prometheus metrics, provisioned Grafana dashboard, Docker/Compose, GitHub Actions, Makefile and pre-commit.
- Unit, protocol, integration, concurrency, persistence, property/fuzz-oriented and performance smoke tests. Slow-subscriber, noeviction, snapshot-write failure, AOF disk-write failure, disconnect-during-response, and active-client shutdown failure modes are covered.
- Architecture, protocol, persistence, design decisions, benchmarks and interview guide documentation.

## Verification in this sandbox
- `python -m pytest -q`: **28 passed, 1 skipped**. The skipped module is Hypothesis-gated because Hypothesis is not installed in this sandbox; it is declared in dev dependencies and runs in CI after dependency installation.
- Python compilation: passed.
- Real TCP smoke test: PING -> PONG.
- Load smoke: 2,000 requests, 20 clients, pipeline 10, 0 errors (see `docs/benchmarks.md`).

## Environment-limited checks
The repository contains the required configuration for Ruff, Black, mypy, Docker, Docker Compose, Prometheus and Grafana. Those executables are not installed in this execution sandbox, so local execution is not falsely claimed. CI is configured to run Ruff, Black, mypy, pytest and Docker build on GitHub.

## Intentional limitations vs Redis
MiniRedis implements a documented Redis-compatible subset. It is string-only, single-writer with optional asynchronous read replicas, uses approximate Python-level memory accounting, has simplified SCAN/transactions/eviction compared with Redis, and is not intended for exposure to the untrusted public Internet. Replication, sharding, heartbeats and leader election: single-primary asynchronous replication (PSYNC handshake, full/partial resync, replica reconnect, read-only enforcement) is now implemented and tested; sharding, consensus-based leader election, and automatic failover remain out of scope and are documented as roadmap items rather than falsely claimed.
