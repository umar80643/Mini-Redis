# Benchmarks

`benchmarks/suite.py` is the single entry point for all workloads. No results are hard-coded anywhere in this repository — every number is a live measurement you produce by running it yourself.

## What it measures

- **SET throughput** — single-key writes at each requested concurrency level.
- **GET throughput** — reads against a pre-seeded key population.
- **Mixed workload** — 80% reads / 20% writes against a shared key population.
- **Pipelined throughput** — many commands sent back-to-back before reading replies, batched.
- **Concurrency levels** — pass any list via `--concurrency`; each level runs all three non-pipelined workloads.

Each row reports: request count, wall-clock duration, ops/sec, p50/p95/p99 latency (ms), the server's self-reported `used_memory` (via `INFO`), and the benchmark client's own peak RSS (as a rough signal of client-side overhead, not server memory).

## Running it

```bash
python -m miniredis &
python benchmarks/suite.py --port 6379 --requests 3000 --concurrency 1 10 50
```

Results are written to `benchmarks/results/benchmark-<timestamp>.json` and `.csv` (gitignored — regenerate locally, don't trust a checked-in number from someone else's machine) and summarized as a plain-text ASCII bar chart on stdout.

## Comparing against real Redis

```bash
docker compose --profile benchmark up -d redis-reference   # Redis on port 6380
python benchmarks/suite.py --port 6379 --compare-port 6380
```

This prints both sets of measurements side by side. It never asserts MiniRedis matches or beats Redis — treat any gap as expected (a from-scratch Python implementation vs. a decades-optimized C one) and report it honestly if you cite these numbers anywhere.

## Reporting results responsibly

If you reference benchmark numbers in a resume, README, or interview, include: CPU model, OS, Python version, MiniRedis config (AOF policy, eviction policy), client/server on the same machine or over a network, request count, concurrency level, and whether this was a cold run or after warm-up. A single unlabeled "50k ops/sec" claim is not verifiable and shouldn't be trusted — including one from this project's own author.
