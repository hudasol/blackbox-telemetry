"""Integration test: proves the worker's main loop backs off and then
recovers from a *generic* Redis error in its own read path — a dropped
connection, a blip, anything that isn't the stream/group itself being
gone (that specific case, NOGROUP, is already covered by
test_worker_consumer_group_recovery.py; this is the other branch of the
same `except RedisError` in worker/main.py).

This exact behavior (docs/PROCESS_LOG.md, Worker entry: "wrapped the
main loop's Redis calls in the same bounded-backoff pattern... so a
transient Redis-level error no longer kills the process either") had
never actually been pinned down by an automated test before this — it
was only ever caught live, by the unrelated socket_timeout bug that
happened to exercise the same code path. Drives the real
worker.main.run() loop, same approach as the NOGROUP test, so what's
under test is the loop's own error handling, not a mocked-out version
of it.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

from redis.exceptions import RedisError
from telemetry_contract import STREAM_NAME, TelemetryEvent
from worker import main as worker_main

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


async def test_worker_backs_off_and_recovers_from_a_transient_redis_error(
    pg_pool, redis_client, monkeypatch
):
    monkeypatch.setenv("REDIS_URL", REDIS_TEST_URL)
    monkeypatch.setenv("POSTGRES_DSN", POSTGRES_TEST_DSN)
    monkeypatch.setenv("WORKER_CONSUMER_NAME", "worker-redis-blip-test")
    # next_backoff_seconds is shared by the per-message Postgres retry
    # loop and this main-loop Redis-error branch — shrink it so the test
    # doesn't have to wait out the real 0.5s/1s/2s schedule.
    monkeypatch.setattr("worker.processing.BACKOFF_INITIAL_SECONDS", 0.02)
    monkeypatch.setattr("worker.processing.BACKOFF_MAX_SECONDS", 0.05)

    real_read_new = worker_main.read_new
    failures_left = 3

    async def flaky_read_new(redis, consumer_name, block_ms):
        nonlocal failures_left
        if failures_left > 0:
            failures_left -= 1
            # A plain RedisError, deliberately not a ResponseError/NOGROUP
            # one — this must land in the generic backoff branch, not the
            # recreate-the-group one.
            raise RedisError("simulated transient redis error")
        return await real_read_new(redis, consumer_name, block_ms)

    monkeypatch.setattr(worker_main, "read_new", flaky_read_new)

    shutdown_event = asyncio.Event()
    task = asyncio.create_task(worker_main.run(shutdown_event))

    try:
        # id="0" consumer groups (ensure_consumer_group) deliver every
        # undelivered message via ">" regardless of exactly when it was
        # added relative to group creation, so enqueueing right away
        # (rather than waiting for startup to finish) is fine.
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
                "worker never recovered from the transient redis error"
            )

        # Confirms the test actually exercised the failure-then-recovery
        # path, not just a lucky first-try success.
        assert failures_left == 0
    finally:
        shutdown_event.set()
        # Same margin as the NOGROUP test: shutdown waits for whatever
        # blocking call (or backoff sleep) is in progress to return.
        await asyncio.wait_for(task, timeout=(worker_main.READ_BLOCK_MS / 1000) + 3)
