#!/usr/bin/env python3
"""Performance proof: a small async load test against the real, running
api + worker + Redis + Postgres stack — no mocks, no hand-waving.

Measures two different things on purpose, because they answer two
different questions:

  1. Ingest latency/throughput — how fast can POST /api/v1/telemetry
     itself respond? This only touches Redis (D4), so it's mostly
     measuring one XADD round-trip.
  2. Drain time — once ingest stops accepting new events, how long does
     the worker take to fully catch up (backlog back to zero)? This is
     the other side of the pipeline: one Postgres insert+upsert
     transaction per message, processed one message at a time
     (worker/main.py's loop reads with count=1 — no concurrency).

Comparing the two is the actual point: it's what turns "the worker might
be a bottleneck" from a guess into a measured fact. See
docs/IMPROVEMENTS.md, "performance proof", for a real run's numbers and
what they showed.

Usage:
    python3 scripts/load_test.py --count 500 --concurrency 50

Requires: the stack up via `docker compose up --build -d` (api on
localhost:8000, worker's metrics on localhost:9100), and httpx installed
(already a dependency of services/api[dev] and services/simulator).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
import uuid
from datetime import datetime, timezone

import httpx


def make_event(asset_id: str, seq: int) -> dict:
    return {
        "asset_id": asset_id,
        "asset_type": "drone",
        "seq": seq,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "position": {"x_m": 1.0, "y_m": 2.0, "z_m": 3.0},
        "speed_mps": 4.2,
        "heading_deg": 180.0,
        "battery_pct": 87.5,
        "temperature_c": 25.0,
        "link_quality_pct": 92.0,
        "mode": "moving",
    }


async def send_one(client: httpx.AsyncClient, api_base: str, asset_id: str, seq: int) -> float:
    """Returns latency in seconds. Raises on anything other than a clean
    202 — a load test that silently ignores failures would produce a
    number that looks good and means nothing."""
    start = time.monotonic()
    response = await client.post(f"{api_base}/api/v1/telemetry", json=make_event(asset_id, seq))
    elapsed = time.monotonic() - start
    if response.status_code != 202:
        raise RuntimeError(f"seq={seq} got HTTP {response.status_code}: {response.text[:200]}")
    return elapsed


async def run_load(api_base: str, asset_id: str, count: int, concurrency: int) -> list[float]:
    latencies: list[float] = []
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(timeout=30.0) as client:

        async def bounded_send(seq: int) -> None:
            async with semaphore:
                latencies.append(await send_one(client, api_base, asset_id, seq))

        # Not a bare asyncio.gather(...): plain gather propagates the
        # first exception the moment it happens, without cancelling the
        # other tasks — they're still mid-`await client.post(...)` when
        # this `async with` block's __aexit__ then closes the client out
        # from under them. Each of those in-flight requests then fails
        # too, but with its own unrelated-looking error (a closed-client
        # or connection-pool exception) layered on top of the one real
        # failure, instead of a single clean report of what went wrong.
        tasks = [asyncio.ensure_future(bounded_send(seq)) for seq in range(count)]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        finally:
            # Runs whether every task finished cleanly, one raised and
            # the wait above returned early, or this coroutine itself
            # got cancelled — either way, make sure every task is
            # actually done (not just no-longer-pending) before the
            # `async with` above is allowed to close the client.
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        for task in tasks:
            if task.cancelled():
                continue
            exc = task.exception()
            if exc is not None:
                raise exc

    return latencies


async def wait_for_drain(api_base: str, asset_id: str, expected_count: int, timeout_s: float) -> float:
    """Polls the query API until every sent event has been stored, and
    returns how long that took from the moment this function was called
    — the worker's real drain time for this batch, not a guess."""
    start = time.monotonic()
    async with httpx.AsyncClient(timeout=10.0) as client:
        while time.monotonic() - start < timeout_s:
            response = await client.get(
                f"{api_base}/api/v1/assets/{asset_id}/history", params={"limit": 500}
            )
            if response.status_code == 200 and response.json()["count"] >= expected_count:
                return time.monotonic() - start
            await asyncio.sleep(0.25)
    raise TimeoutError(
        f"worker did not drain {expected_count} events within {timeout_s}s"
    )


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, int(len(sorted_values) * pct))
    return sorted_values[index]


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-base", default="http://localhost:8000")
    parser.add_argument("--count", type=int, default=500, help="total events to send")
    parser.add_argument("--concurrency", type=int, default=50, help="parallel ingest requests")
    parser.add_argument(
        "--drain-timeout", type=float, default=120.0, help="max seconds to wait for the worker to catch up"
    )
    parser.add_argument("--out", default=None, help="write results as JSON to this path")
    args = parser.parse_args()

    asset_id = f"loadtest-{uuid.uuid4().hex[:8]}"
    print(f"[load-test] asset_id={asset_id} count={args.count} concurrency={args.concurrency}")

    wall_start = time.monotonic()
    try:
        latencies = await run_load(args.api_base, asset_id, args.count, args.concurrency)
    except RuntimeError as exc:
        print(f"[load-test] FAIL: {exc}", file=sys.stderr)
        return 1
    ingest_wall_seconds = time.monotonic() - wall_start

    latencies.sort()
    throughput = args.count / ingest_wall_seconds
    p50 = percentile(latencies, 0.50)
    p95 = percentile(latencies, 0.95)
    p99 = percentile(latencies, 0.99)
    mean = statistics.mean(latencies)

    print(f"[load-test] ingest: {args.count} events in {ingest_wall_seconds:.2f}s "
          f"= {throughput:.1f} req/s")
    print(f"[load-test] ingest latency: p50={p50*1000:.1f}ms p95={p95*1000:.1f}ms "
          f"p99={p99*1000:.1f}ms mean={mean*1000:.1f}ms")

    print(f"[load-test] waiting for the worker to drain the backlog (up to {args.drain_timeout:.0f}s)...")
    try:
        drain_seconds = await wait_for_drain(args.api_base, asset_id, args.count, args.drain_timeout)
    except TimeoutError as exc:
        print(f"[load-test] FAIL: {exc}", file=sys.stderr)
        return 1
    worker_throughput = args.count / drain_seconds
    print(f"[load-test] worker drain: {args.count} events fully stored in {drain_seconds:.2f}s "
          f"= {worker_throughput:.1f} events/s")

    if worker_throughput < throughput:
        bottleneck = "worker (Postgres insert path)"
        print(f"[load-test] bottleneck: {bottleneck} — ingest accepts at "
              f"{throughput:.1f}/s, worker only drains at {worker_throughput:.1f}/s")
    else:
        bottleneck = "ingest (Redis XADD path)"
        print(f"[load-test] worker keeps up with ingest at this load "
              f"({worker_throughput:.1f}/s >= {throughput:.1f}/s)")

    result = {
        "asset_id": asset_id,
        "count": args.count,
        "concurrency": args.concurrency,
        "ingest_wall_seconds": ingest_wall_seconds,
        "ingest_throughput_per_s": throughput,
        "ingest_latency_p50_ms": p50 * 1000,
        "ingest_latency_p95_ms": p95 * 1000,
        "ingest_latency_p99_ms": p99 * 1000,
        "ingest_latency_mean_ms": mean * 1000,
        "worker_drain_seconds": drain_seconds,
        "worker_throughput_per_s": worker_throughput,
        "bottleneck": bottleneck,
    }
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2)
        print(f"[load-test] wrote {args.out}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
