"""Integration tests for worker.processing.process_message — the piece
that ties Redis Streams, Postgres, and the retry/ack rules together.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from telemetry_contract import DEADLETTER_STREAM_NAME, STREAM_NAME, TelemetryEvent
from worker import db
from worker.processing import process_message
from worker.redis_stream import (
    acquire_message_lock,
    ensure_consumer_group,
    extend_message_lock,
    release_message_lock,
)


def valid_payload(**overrides: object) -> dict:
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
    return payload


async def enqueue(redis_client, **overrides: object) -> str:
    event = TelemetryEvent(**valid_payload(**overrides))
    return await redis_client.xadd(
        STREAM_NAME,
        {
            "asset_id": event.asset_id,
            "seq": str(event.seq),
            "received_at": datetime.now(timezone.utc).isoformat(),
            "payload": event.model_dump_json(),
        },
    )


async def read_one(redis_client, consumer="test-consumer"):
    await ensure_consumer_group(redis_client)
    response = await redis_client.xreadgroup(
        "worker-group", consumer, {STREAM_NAME: ">"}, count=1
    )
    _stream, messages = response[0]
    return messages[0]  # (message_id, fields)


async def test_valid_message_is_stored_and_acked(pg_pool, redis_client):
    await enqueue(redis_client, seq=1)
    message_id, fields = await read_one(redis_client)

    await process_message(
        redis_client, pg_pool, message_id, fields, asyncio.Event()
    )

    row = await pg_pool.fetchrow(
        "SELECT * FROM telemetry_events WHERE asset_id = $1 AND seq = $2",
        "drone-01",
        1,
    )
    assert row is not None

    pending = await redis_client.xpending(STREAM_NAME, "worker-group")
    assert pending["pending"] == 0  # acked, nothing left outstanding


async def test_malformed_payload_is_logged_and_acked_not_retried_forever(
    pg_pool, redis_client
):
    # Simulates a corrupted/poison stream entry that could never have come
    # from the real ingest API (which validates first) — e.g. a hand-
    # crafted XADD, or a future incompatible contract change.
    await ensure_consumer_group(redis_client)
    message_id = await redis_client.xadd(STREAM_NAME, {"payload": "not valid json"})
    response = await redis_client.xreadgroup(
        "worker-group", "test-consumer", {STREAM_NAME: ">"}, count=1
    )
    _stream, messages = response[0]
    message_id, fields = messages[0]

    await process_message(
        redis_client, pg_pool, message_id, fields, asyncio.Event()
    )

    pending = await redis_client.xpending(STREAM_NAME, "worker-group")
    assert pending["pending"] == 0  # acked immediately, not left stuck

    count = await pg_pool.fetchval("SELECT count(*) FROM telemetry_events")
    assert count == 0  # nothing sane to store

    # Not just discarded, though: the original bytes are preserved on the
    # dead-letter stream so an operator can inspect/replay it (see
    # test_deadletter_replay_puts_the_message_back_for_reprocessing below,
    # and docs/IMPROVEMENTS.md).
    deadlettered = await redis_client.xrange(DEADLETTER_STREAM_NAME)
    assert len(deadlettered) == 1
    _entry_id, dl_fields = deadlettered[0]
    assert dl_fields["original_message_id"] == message_id
    assert dl_fields["payload"] == "not valid json"
    assert "reason" in dl_fields and dl_fields["reason"]  # the parse error, not blank


async def test_deadletter_replay_puts_the_message_back_for_reprocessing(
    pg_pool, redis_client
):
    """The other half of poison-message handling: once a dead-lettered
    entry is replayed (api/routers/deadletter.py's job — exercised here
    directly against Redis, the same primitives that router uses), the
    worker processes it exactly like any other message. A payload that's
    valid *now* (e.g. hand-fixed before replay) is stored normally."""
    await ensure_consumer_group(redis_client)
    bad_message_id = await redis_client.xadd(STREAM_NAME, {"payload": "still not json"})
    response = await redis_client.xreadgroup(
        "worker-group", "test-consumer", {STREAM_NAME: ">"}, count=1
    )
    _stream, messages = response[0]
    message_id, fields = messages[0]
    await process_message(redis_client, pg_pool, message_id, fields, asyncio.Event())

    deadlettered = await redis_client.xrange(DEADLETTER_STREAM_NAME)
    assert len(deadlettered) == 1
    entry_id, dl_fields = deadlettered[0]
    assert dl_fields["original_message_id"] == bad_message_id

    # Replay: put a *now-valid* payload back on telemetry.raw under the
    # same asset_id/seq bookkeeping, then drop the dead-letter entry —
    # exactly what POST .../deadletter/{id}/replay does.
    fixed_event = TelemetryEvent(**valid_payload(seq=7))
    await redis_client.xadd(
        STREAM_NAME,
        {
            "asset_id": fixed_event.asset_id,
            "seq": str(fixed_event.seq),
            "received_at": dl_fields["received_at"] or datetime.now(timezone.utc).isoformat(),
            "payload": fixed_event.model_dump_json(),
        },
    )
    await redis_client.xdel(DEADLETTER_STREAM_NAME, entry_id)

    replayed_message_id, replayed_fields = await read_one(redis_client, consumer="replay-consumer")
    await process_message(
        redis_client, pg_pool, replayed_message_id, replayed_fields, asyncio.Event()
    )

    row = await pg_pool.fetchrow(
        "SELECT * FROM telemetry_events WHERE asset_id = $1 AND seq = $2",
        "drone-01",
        7,
    )
    assert row is not None  # the replayed event landed
    assert await redis_client.xrange(DEADLETTER_STREAM_NAME) == []  # and is gone from dead-letter


async def test_nul_byte_asset_id_is_deadlettered_at_parse_not_crashed(
    pg_pool, redis_client
):
    """A NUL byte in asset_id used to pass validation untouched and only
    fail once db.store_event tried to write it — as a plain ValueError
    from asyncpg that nothing caught, crashing the worker outright (and,
    since the message was never acked, crashing it again on every
    restart once claim_stale_pending reclaimed it). It's now rejected by
    telemetry_contract.TelemetryEvent itself, so process_message's
    existing parse-validation dead-letter path catches it like any other
    malformed message — before ever touching Postgres."""
    await ensure_consumer_group(redis_client)
    bad_payload = json.dumps({**valid_payload(), "asset_id": "drone\x00-01"})
    await redis_client.xadd(STREAM_NAME, {"payload": bad_payload})
    response = await redis_client.xreadgroup(
        "worker-group", "test-consumer", {STREAM_NAME: ">"}, count=1
    )
    _stream, messages = response[0]
    message_id, fields = messages[0]

    await process_message(
        redis_client, pg_pool, message_id, fields, asyncio.Event()
    )

    pending = await redis_client.xpending(STREAM_NAME, "worker-group")
    assert pending["pending"] == 0  # acked, not crashed and not stuck

    count = await pg_pool.fetchval("SELECT count(*) FROM telemetry_events")
    assert count == 0  # never reached the database

    deadlettered = await redis_client.xrange(DEADLETTER_STREAM_NAME)
    assert len(deadlettered) == 1
    assert deadlettered[0][1]["original_message_id"] == message_id


async def test_non_finite_extra_field_is_deadlettered_at_parse_not_retried_forever(
    pg_pool, redis_client
):
    """Same failure class as the NUL-byte case, for extra="allow"
    fields: a NaN/Infinity used to serialize fine (json.dumps allows it
    by default) and only fail at Postgres's strict jsonb parser — on
    every retry, identically, since the payload never changes. Now
    rejected at parse time instead, so it's one dead-lettered message,
    not an unbounded retry loop blocking every message behind it (the
    worker processes one at a time)."""
    await ensure_consumer_group(redis_client)
    bad_payload = json.dumps({**valid_payload(), "signal_dbm": float("nan")})
    await redis_client.xadd(STREAM_NAME, {"payload": bad_payload})
    response = await redis_client.xreadgroup(
        "worker-group", "test-consumer", {STREAM_NAME: ">"}, count=1
    )
    _stream, messages = response[0]
    message_id, fields = messages[0]

    await process_message(
        redis_client, pg_pool, message_id, fields, asyncio.Event()
    )

    pending = await redis_client.xpending(STREAM_NAME, "worker-group")
    assert pending["pending"] == 0
    count = await pg_pool.fetchval("SELECT count(*) FROM telemetry_events")
    assert count == 0
    deadlettered = await redis_client.xrange(DEADLETTER_STREAM_NAME)
    assert len(deadlettered) == 1


async def test_store_time_value_error_is_deadlettered_not_retried_forever(
    pg_pool, redis_client, monkeypatch
):
    """Defense-in-depth: even if a ValueError somehow reaches
    db.store_event (the contract validation above should prevent this
    in practice — this simulates a gap in it), the worker must treat it
    as permanent — dead-letter and move on — never retry it forever
    (wedging the queue) and never let it propagate uncaught (crashing
    the worker, which would just reprocess and crash again on the next
    restart)."""
    await enqueue(redis_client, seq=1)
    message_id, fields = await read_one(redis_client)

    async def always_raises_value_error(pool, event, received_at):
        raise ValueError("simulated client-side encoding failure")

    monkeypatch.setattr(db, "store_event", always_raises_value_error)

    await process_message(
        redis_client, pg_pool, message_id, fields, asyncio.Event()
    )

    pending = await redis_client.xpending(STREAM_NAME, "worker-group")
    assert pending["pending"] == 0  # acked, not left retrying forever
    count = await pg_pool.fetchval("SELECT count(*) FROM telemetry_events")
    assert count == 0
    deadlettered = await redis_client.xrange(DEADLETTER_STREAM_NAME)
    assert len(deadlettered) == 1
    assert deadlettered[0][1]["original_message_id"] == message_id


async def test_acquire_message_lock_is_exclusive(redis_client):
    first = await acquire_message_lock(redis_client, "msg-1", "token-a", 5)
    second = await acquire_message_lock(redis_client, "msg-1", "token-b", 5)
    assert first is True
    assert second is False  # already held by token-a


async def test_extend_message_lock_only_succeeds_for_the_current_holder(redis_client):
    await acquire_message_lock(redis_client, "msg-1", "token-a", 5)
    assert await extend_message_lock(redis_client, "msg-1", "token-a", 5) is True
    # A different token (a different attempt — its own lock has since
    # expired and someone else's is live, or it never held one at all)
    # must never be able to extend someone else's lock.
    assert await extend_message_lock(redis_client, "msg-1", "wrong-token", 5) is False


async def test_release_message_lock_only_succeeds_for_the_current_holder(redis_client):
    await acquire_message_lock(redis_client, "msg-1", "token-a", 5)

    await release_message_lock(redis_client, "msg-1", "wrong-token")
    assert await acquire_message_lock(redis_client, "msg-1", "token-b", 5) is False  # still held

    await release_message_lock(redis_client, "msg-1", "token-a")
    assert await acquire_message_lock(redis_client, "msg-1", "token-c", 5) is True  # now free


async def test_expired_message_lock_can_be_reacquired(redis_client):
    await acquire_message_lock(redis_client, "msg-1", "token-a", 1)
    await asyncio.sleep(1.2)
    assert await acquire_message_lock(redis_client, "msg-1", "token-b", 5) is True


async def test_concurrent_processing_by_two_replicas_only_stores_once(
    pg_pool, redis_client, monkeypatch
):
    """The bug this closes: claim_stale_pending's idle-time threshold
    (worker/main.py's CLAIM_MIN_IDLE_MS) has no way to know a message
    is still being actively retried by another worker replica — with
    more than one replica running (worker/main.py's own comments
    anticipate --scale worker=N), a second replica's XAUTOCLAIM can
    legitimately reclaim the exact same message mid-retry and start
    processing it too. Simulated here by calling process_message twice,
    concurrently, for the identical message_id/fields — the shape both
    a genuinely-slow first attempt and a same-message reclaim produce.
    Only the winner should ever reach db.store_event; the other must
    back off untouched, not race it."""
    await enqueue(redis_client, seq=1)
    message_id, fields = await read_one(redis_client)

    real_store_event = db.store_event
    call_count = 0

    async def slow_store_event(pool, event, received_at):
        nonlocal call_count
        call_count += 1
        # Held open long enough that both concurrent process_message
        # calls are guaranteed to have already reached this point (or
        # be blocked before it, contending on the lock) — proving the
        # lock, not scheduling luck, is what keeps the loser out.
        await asyncio.sleep(0.05)
        return await real_store_event(pool, event, received_at)

    monkeypatch.setattr(db, "store_event", slow_store_event)

    await asyncio.gather(
        process_message(redis_client, pg_pool, message_id, fields, asyncio.Event()),
        process_message(redis_client, pg_pool, message_id, fields, asyncio.Event()),
    )

    assert call_count == 1  # the losing attempt never reached db.store_event at all

    count = await pg_pool.fetchval(
        "SELECT count(*) FROM telemetry_events WHERE asset_id = $1 AND seq = $2",
        "drone-01",
        1,
    )
    assert count == 1

    pending = await redis_client.xpending(STREAM_NAME, "worker-group")
    assert pending["pending"] == 0  # the winner acked it; the loser never touched it

    assert await redis_client.xrange(DEADLETTER_STREAM_NAME) == []


async def test_duplicate_message_is_acked_without_a_second_row(pg_pool, redis_client):
    await enqueue(redis_client, seq=9)
    message_id, fields = await read_one(redis_client)
    await process_message(redis_client, pg_pool, message_id, fields, asyncio.Event())

    # A second, independent message carrying the same logical event
    # (e.g. the API enqueued it twice because the client retried).
    await enqueue(redis_client, seq=9)
    message_id_2, fields_2 = await read_one(redis_client)
    await process_message(
        redis_client, pg_pool, message_id_2, fields_2, asyncio.Event()
    )

    count = await pg_pool.fetchval(
        "SELECT count(*) FROM telemetry_events WHERE asset_id = $1 AND seq = $2",
        "drone-01",
        9,
    )
    assert count == 1
    pending = await redis_client.xpending(STREAM_NAME, "worker-group")
    assert pending["pending"] == 0  # both messages acked


async def test_db_failure_is_retried_and_not_acked_until_it_succeeds(
    pg_pool, redis_client, monkeypatch
):
    await enqueue(redis_client, seq=1)
    message_id, fields = await read_one(redis_client)

    real_store_event = db.store_event
    call_count = 0

    async def flaky_store_event(pool, event, received_at):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("simulated postgres outage")
        return await real_store_event(pool, event, received_at)

    monkeypatch.setattr(db, "store_event", flaky_store_event)
    # Speed the test up — same code path, just not sleeping 0.5/1/2s.
    monkeypatch.setattr("worker.processing.BACKOFF_INITIAL_SECONDS", 0.01)
    monkeypatch.setattr("worker.processing.BACKOFF_MAX_SECONDS", 0.05)

    await process_message(
        redis_client, pg_pool, message_id, fields, asyncio.Event()
    )

    assert call_count == 3  # failed twice, succeeded on the third attempt
    row = await pg_pool.fetchrow(
        "SELECT * FROM telemetry_events WHERE asset_id = $1 AND seq = $2",
        "drone-01",
        1,
    )
    assert row is not None  # eventually stored
    pending = await redis_client.xpending(STREAM_NAME, "worker-group")
    assert pending["pending"] == 0  # acked only after success


async def test_shutdown_mid_retry_leaves_message_unacked(pg_pool, redis_client, monkeypatch):
    await enqueue(redis_client, seq=1)
    message_id, fields = await read_one(redis_client)

    async def always_fails(pool, event, received_at):
        raise ConnectionError("simulated postgres outage")

    monkeypatch.setattr(db, "store_event", always_fails)
    monkeypatch.setattr("worker.processing.BACKOFF_INITIAL_SECONDS", 0.01)

    shutdown_event = asyncio.Event()
    shutdown_event.set()  # already shutting down before we even start

    await process_message(redis_client, pg_pool, message_id, fields, shutdown_event)

    # Never stored, and — crucially — never acked either. It stays in the
    # Pending Entries List for a future claim_stale_pending to recover.
    count = await pg_pool.fetchval("SELECT count(*) FROM telemetry_events")
    assert count == 0
    pending = await redis_client.xpending(STREAM_NAME, "worker-group")
    assert pending["pending"] == 1
