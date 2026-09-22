"""Integration tests for the "worker killed and restarted" failure case
(docs/PLAN.md) — the one the brief specifically calls out as something it
will test live. Proves claim_stale_pending() + idempotent storage means a
crash recovers cleanly, whether the crash happened before or after the
Postgres write actually landed.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from telemetry_contract import STREAM_NAME, TelemetryEvent
from worker import db
from worker.processing import process_message
from worker.redis_stream import claim_stale_pending, ensure_consumer_group

MIN_IDLE_MS = 20  # short, so the test doesn't have to sleep 30s for real


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


async def test_message_read_but_never_processed_is_reclaimed_after_crash(
    pg_pool, redis_client
):
    event = make_event(seq=1)
    await enqueue(redis_client, event)
    await ensure_consumer_group(redis_client)

    # "worker-crashed" reads the message and then dies — never calls
    # process_message at all.
    response = await redis_client.xreadgroup(
        "worker-group", "worker-crashed", {STREAM_NAME: ">"}, count=1
    )
    _stream, messages = response[0]
    crashed_message_id = messages[0][0]

    await asyncio.sleep(MIN_IDLE_MS / 1000 + 0.05)

    # A replacement worker starts up under a different consumer name and
    # sweeps for abandoned work, exactly like main.run() does on startup.
    reclaimed = await claim_stale_pending(redis_client, "worker-restarted", MIN_IDLE_MS)

    assert len(reclaimed) == 1
    message_id, fields = reclaimed[0]
    assert message_id == crashed_message_id

    await process_message(redis_client, pg_pool, message_id, fields, asyncio.Event())

    row = await pg_pool.fetchrow(
        "SELECT * FROM telemetry_events WHERE asset_id = $1 AND seq = $2",
        "drone-01",
        1,
    )
    assert row is not None
    pending = await redis_client.xpending(STREAM_NAME, "worker-group")
    assert pending["pending"] == 0


async def test_message_already_stored_before_crash_is_recovered_without_duplicating(
    pg_pool, redis_client
):
    """The narrower, more important case: the worker successfully wrote
    the row to Postgres and crashed in the gap before it could XACK. On
    restart the message is still pending (never acked) and gets
    reclaimed — reprocessing it must not create a second row, and must
    finally ack it so it stops coming back forever."""
    event = make_event(seq=42)
    await enqueue(redis_client, event)
    await ensure_consumer_group(redis_client)

    response = await redis_client.xreadgroup(
        "worker-group", "worker-crashed", {STREAM_NAME: ">"}, count=1
    )
    _stream, messages = response[0]
    message_id, fields = messages[0]

    # The write actually happens (this is the part a real crash wouldn't
    # undo — Postgres already committed)...
    await db.store_event(pg_pool, event, datetime.now(timezone.utc))
    # ...but the crash happens right here, before ack() is ever called.

    await asyncio.sleep(MIN_IDLE_MS / 1000 + 0.05)
    reclaimed = await claim_stale_pending(redis_client, "worker-restarted", MIN_IDLE_MS)
    assert len(reclaimed) == 1
    reclaimed_id, reclaimed_fields = reclaimed[0]

    await process_message(
        redis_client, pg_pool, reclaimed_id, reclaimed_fields, asyncio.Event()
    )

    count = await pg_pool.fetchval(
        "SELECT count(*) FROM telemetry_events WHERE asset_id = $1 AND seq = $2",
        "drone-01",
        42,
    )
    assert count == 1  # not duplicated by reprocessing
    pending = await redis_client.xpending(STREAM_NAME, "worker-group")
    assert pending["pending"] == 0  # and now actually acked
