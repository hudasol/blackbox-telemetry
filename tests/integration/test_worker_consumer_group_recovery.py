"""Integration test: proves the worker recovers on its own when the
consumer group disappears out from under it — a Redis restart without
persistence, or an operator flush — even though Redis itself stays
reachable the whole time.

Found live while smoke-testing the query API (an unrelated flushdb from
a separate test run against the same throwaway Redis wiped a running
worker's stream mid-demo) — see docs/PROCESS_LOG.md. Drives the real
worker.main.run() loop as a background task, since the bug was in how
the loop reacts to a NOGROUP error, not in any one pure function.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

from redis.exceptions import ResponseError
from telemetry_contract import CONSUMER_GROUP_NAME, STREAM_NAME, TelemetryEvent
from worker import main as worker_main

# Same env vars and defaults as conftest.py's pg_pool/redis_client
# fixtures — kept local rather than imported, since tests/integration
# isn't a package (no __init__.py), matching this repo's other test
# files.
POSTGRES_TEST_DSN = os.environ.get(
    "POSTGRES_TEST_DSN", "postgresql://blackbox:blackbox@localhost:5432/blackbox_test"
)
REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:16379/0")


def make_event(**overrides: object) -> TelemetryEvent:
    payload = {
        "asset_id": "drone-01",
        "asset_type": "drone",
        "seq": 1,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "position": {"x_m": 1.0, "y_m": 2.0, "z_m": 3.0},
        "speed_mps": 4.2,
        "heading_deg": 180.0,
        "battery_pct": 87.5,
        "temperature_c": 25.0,
        "link_quality_pct": 92.0,
        "mode": "moving",
    }
    payload.update(overrides)
    return TelemetryEvent(**payload)


async def enqueue(redis_client, event: TelemetryEvent) -> str:
    return await redis_client.xadd(
        STREAM_NAME,
        {
            "asset_id": event.asset_id,
            "seq": str(event.seq),
            "received_at": datetime.now(timezone.utc).isoformat(),
            "payload": event.model_dump_json(),
        },
    )


async def test_worker_recreates_a_deleted_consumer_group_and_keeps_going(
    pg_pool, redis_client, monkeypatch
):
    monkeypatch.setenv("REDIS_URL", REDIS_TEST_URL)
    monkeypatch.setenv("POSTGRES_DSN", POSTGRES_TEST_DSN)
    monkeypatch.setenv("WORKER_CONSUMER_NAME", "worker-self-heal-test")

    shutdown_event = asyncio.Event()
    task = asyncio.create_task(worker_main.run(shutdown_event))

    try:
        # Wait for the worker's own startup to actually create the group
        # (rather than a fixed sleep) — otherwise deleting the stream too
        # early would just be its first-ever creation, not a recreation,
        # and this wouldn't be testing the recovery path at all.
        for _ in range(50):
            try:
                groups = await redis_client.xinfo_groups(STREAM_NAME)
                if any(g["name"] == CONSUMER_GROUP_NAME for g in groups):
                    break
            except ResponseError:
                pass
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("worker never created the consumer group at startup")

        # Simulate the group vanishing out from under a running worker.
        await redis_client.delete(STREAM_NAME)

        # A fresh event arrives after the group is gone — the old code
        # would retry NOGROUP forever and never see it.
        event = make_event(seq=1)
        await enqueue(redis_client, event)

        for _ in range(80):  # up to ~8s
            row = await pg_pool.fetchrow(
                "SELECT 1 FROM telemetry_events WHERE asset_id = $1 AND seq = $2",
                "drone-01",
                1,
            )
            if row is not None:
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError(
                "worker never recovered from the missing consumer group"
            )
    finally:
        shutdown_event.set()
        # The loop only checks shutdown_event between iterations, not
        # mid-call — if it's inside a blocking XREADGROUP (up to
        # READ_BLOCK_MS) when shutdown is requested, exit waits for that
        # call to return first. Bounded, known, and fine in practice
        # (typical orchestrator grace periods are far longer), but this
        # wait needs enough margin not to fail on that alone.
        await asyncio.wait_for(task, timeout=(worker_main.READ_BLOCK_MS / 1000) + 3)
