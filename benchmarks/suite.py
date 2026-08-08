"""Professional benchmark suite for MiniRedis.

Measures GET throughput, SET throughput, a mixed 80%-read/20%-write
workload, pipelined throughput, and behavior across several concurrency
levels. Every number is a live measurement against a running server -
nothing here is hard-coded. Results are written as both JSON and CSV to
benchmarks/results/, plus a plain-text throughput summary on stdout.

Usage:
    # terminal 1
    python -m miniredis

    # terminal 2
    python benchmarks/suite.py --port 6379
    python benchmarks/suite.py --port 6379 --concurrency 1 5 20 100 --requests 3000

Optionally compare against a real Redis instance running alongside:
    python benchmarks/suite.py --port 6379 --compare-port 6380
This never asserts MiniRedis matches or beats Redis - it just reports
both sets of measurements side by side so you can see the real gap.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import resource
import statistics
import sys
import time
from pathlib import Path

from client import BenchClient

RESULTS_DIR = Path(__file__).parent / "results"


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, int(len(sorted_values) * p))
    return sorted_values[index]


async def run_workload(host, port, make_command, requests_per_client, concurrency):
    """Run `concurrency` concurrent clients, each issuing `requests_per_client`
    single (non-pipelined) requests. Returns (sorted latencies in ms, wall-clock seconds).
    """
    clients = [BenchClient(host, port) for _ in range(concurrency)]
    await asyncio.gather(*(c.connect() for c in clients))

    per_worker_latencies: list[list[float]] = [[] for _ in clients]

    async def worker(client, worker_index):
        local = per_worker_latencies[worker_index]
        for i in range(requests_per_client):
            command = make_command(worker_index * requests_per_client + i)
            started = time.perf_counter()
            await client.request(command)
            local.append((time.perf_counter() - started) * 1000)

    started = time.perf_counter()
    await asyncio.gather(*(worker(c, i) for i, c in enumerate(clients)))
    duration = time.perf_counter() - started

    await asyncio.gather(*(c.close() for c in clients))
    latencies = sorted(v for worker_lat in per_worker_latencies for v in worker_lat)
    return latencies, duration


async def run_pipelined(host, port, make_command, total_requests, batch_size):
    client = BenchClient(host, port)
    await client.connect()
    started = time.perf_counter()
    sent = 0
    while sent < total_requests:
        batch = [make_command(sent + j) for j in range(min(batch_size, total_requests - sent))]
        await client.pipeline(batch)
        sent += len(batch)
    duration = time.perf_counter() - started
    await client.close()
    return sent, duration


def summarize(name, concurrency, latencies, duration, **extra):
    row = {
        "benchmark": name,
        "concurrency": concurrency,
        "requests": len(latencies),
        "duration_s": round(duration, 4),
        "ops_per_sec": round(len(latencies) / duration, 2) if duration > 0 else 0.0,
        "p50_ms": round(percentile(latencies, 0.50), 4) if latencies else None,
        "p95_ms": round(percentile(latencies, 0.95), 4) if latencies else None,
        "p99_ms": round(percentile(latencies, 0.99), 4) if latencies else None,
        "mean_ms": round(statistics.mean(latencies), 4) if latencies else None,
    }
    row.update(extra)
    return row


def client_peak_rss_mb() -> float:
    """This benchmark process's own peak RSS, as one signal of client-side overhead.
    The server's memory is measured separately via INFO (see fetch_server_memory)."""
    kb_or_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return kb_or_bytes / 1024 if sys.platform == "linux" else kb_or_bytes / (1024 * 1024)


async def fetch_server_memory_bytes(host, port) -> int | None:
    client = BenchClient(host, port)
    try:
        await client.connect()
        reply = await client.request([b"INFO"])
        await client.close()
    except (ConnectionError, OSError):
        return None
    for line in reply.value.decode().splitlines():
        if line.startswith("used_memory:"):
            return int(line.split(":")[1])
    return None


async def seed_keys(host, port, count):
    client = BenchClient(host, port)
    await client.connect()
    for i in range(count):
        await client.request([b"SET", f"bench:key:{i}".encode(), b"value"])
    await client.close()


async def run_all(
    host, port, requests_per_client, concurrency_levels, pipeline_batch, pipeline_total, label=""
):
    rows = []
    await seed_keys(host, port, 1000)

    for concurrency in concurrency_levels:
        print(f"\n== {label + ' ' if label else ''}concurrency={concurrency} ==")

        lat, dur = await run_workload(
            host,
            port,
            lambda i: [b"SET", f"bench:set:{i}".encode(), b"value"],
            requests_per_client,
            concurrency,
        )
        row = summarize("set", concurrency, lat, dur)
        rows.append(row)
        print("SET  ", row)

        lat, dur = await run_workload(
            host,
            port,
            lambda i: [b"GET", f"bench:key:{i % 1000}".encode()],
            requests_per_client,
            concurrency,
        )
        row = summarize("get", concurrency, lat, dur)
        rows.append(row)
        print("GET  ", row)

        def mixed_command(i):
            key = f"bench:mixed:{i % 1000}".encode()
            return [b"SET", key, b"v"] if random.random() < 0.2 else [b"GET", key]

        lat, dur = await run_workload(host, port, mixed_command, requests_per_client, concurrency)
        row = summarize("mixed_80r_20w", concurrency, lat, dur)
        rows.append(row)
        print("MIXED", row)

    sent, dur = await run_pipelined(
        host,
        port,
        lambda i: [b"SET", f"bench:pipe:{i}".encode(), b"value"],
        pipeline_total,
        pipeline_batch,
    )
    row = summarize(
        "pipelined_set",
        1,
        [],
        dur,
        requests=sent,
        ops_per_sec=round(sent / dur, 2) if dur > 0 else 0.0,
        pipeline_batch=pipeline_batch,
    )
    rows.append(row)
    print("\nPIPELINE", row)

    server_memory = await fetch_server_memory_bytes(host, port)
    for row in rows:
        row["server_used_memory_bytes"] = server_memory
        row["client_peak_rss_mb"] = round(client_peak_rss_mb(), 2)
        if label:
            row["target"] = label
    return rows


def write_results(rows: list[dict]) -> tuple[Path, Path]:
    RESULTS_DIR.mkdir(exist_ok=True)
    timestamp = time.strftime("%Y%m%dT%H%M%S")
    json_path = RESULTS_DIR / f"benchmark-{timestamp}.json"
    csv_path = RESULTS_DIR / f"benchmark-{timestamp}.csv"

    json_path.write_text(json.dumps(rows, indent=2))
    fieldnames = sorted({key for row in rows for key in row})
    with open(csv_path, "w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return json_path, csv_path


def print_ascii_chart(rows: list[dict]) -> None:
    """A dependency-free ASCII bar chart of ops/sec, since pulling in matplotlib
    for one summary chart isn't worth a hard new dependency for this project."""
    throughput_rows = [r for r in rows if r.get("ops_per_sec")]
    if not throughput_rows:
        return
    max_ops = max(r["ops_per_sec"] for r in throughput_rows)
    print("\nThroughput (ops/sec):")
    for row in throughput_rows:
        label = f"{row.get('target', '')}{row['benchmark']}@c{row['concurrency']}"
        bar_width = int((row["ops_per_sec"] / max_ops) * 50) if max_ops else 0
        print(f"  {label:28s} {'#' * bar_width} {row['ops_per_sec']:.0f}")


async def main():
    parser = argparse.ArgumentParser(description="MiniRedis benchmark suite")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument(
        "--requests", type=int, default=3000, help="requests per client, per concurrency level"
    )
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 10, 50])
    parser.add_argument("--pipeline-batch", type=int, default=50)
    parser.add_argument("--pipeline-total", type=int, default=10000)
    parser.add_argument(
        "--compare-port",
        type=int,
        default=None,
        help="also benchmark a Redis instance on this port",
    )
    args = parser.parse_args()

    rows = await run_all(
        args.host,
        args.port,
        args.requests,
        args.concurrency,
        args.pipeline_batch,
        args.pipeline_total,
        label="miniredis:",
    )

    if args.compare_port is not None:
        try:
            redis_rows = await run_all(
                args.host,
                args.compare_port,
                args.requests,
                args.concurrency,
                args.pipeline_batch,
                args.pipeline_total,
                label="redis:",
            )
            rows.extend(redis_rows)
        except (ConnectionError, OSError) as exc:
            print(
                f"\nRedis comparison skipped: could not connect to port {args.compare_port} ({exc})"
            )

    json_path, csv_path = write_results(rows)
    print(f"\nResults written to:\n  {json_path}\n  {csv_path}")
    print_ascii_chart(rows)


if __name__ == "__main__":
    asyncio.run(main())
