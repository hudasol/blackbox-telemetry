"""Integration tests for GET /api/v1/stream (SSE) — against real Redis
Pub/Sub and the worker's actual process_message, so this proves the
whole live-update path end to end: worker stores+acks an event,
publishes it, and a subscriber receives it.

Drives app.routers.stream._event_source directly rather than through a
real HTTP request: httpx's ASGITransport (used everywhere else in this
suite — see test_query_api.py etc.) buffers a StreamingResponse's entire
body before handing any of it back to the client, so it can't be used to
test a generator that (by design, like this one) runs until the client
disconnects — verified empirically while writing this test, not assumed.
Calling the generator function directly is still a real test of this
router's actual logic (StreamingResponse only wraps it, unchanged) and
still exercises real Redis Pub/Sub and the real worker publish call
site — only the HTTP transport layer itself is skipped.

Synchronization note: Pub/Sub has no history/replay — a publish before a
subscriber has subscribed is simply lost (see
worker/redis_stream.py:publish_state_update's docstring). Each test below
starts consuming the generator (which subscribes immediately) and gives
it a brief moment to reach that point before triggering the publish it
expects to see, the same way a real client connects first and then waits.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from app.routers.stream import _event_source
from telemetry_contract import TelemetryEvent
from worker.processing import process_message
from worker.redis_stream import ensure_consumer_group

SUBSCRIBE_SETTLE_SECONDS = 0.3
READ_TIMEOUT_SECONDS = 5.0


class _FakeRequest:
    """The router only ever calls request.is_disconnected() — a stub with
    just that is enough to drive _event_source without a real ASGI scope."""

    async def is_disconnected(self) -> bool:
        return False


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


async def _store_via_worker(pg_pool, redis_client, **overrides: object) -> None:
    """Goes through the real worker path (not a raw PUBLISH) so this
    test proves the actual publish call site (worker/processing.py)
    works, not just that the router can read whatever's handed to it."""
    await ensure_consumer_group(redis_client)
    event = TelemetryEvent(**valid_payload(**overrides))
    await redis_client.xadd(
        "telemetry.raw",
        {
            "asset_id": event.asset_id,
            "seq": str(event.seq),
            "received_at": datetime.now(timezone.utc).isoformat(),
            "payload": event.model_dump_json(),
        },
    )
    response = await redis_client.xreadgroup(
        "worker-group", "stream-test-consumer", {"telemetry.raw": ">"}, count=1
    )
    _stream, messages = response[0]
    message_id, fields = messages[0]
    await process_message(redis_client, pg_pool, message_id, fields, asyncio.Event())


async def _first_event(redis_client, asset_id: str | None) -> dict:
    gen = _event_source(_FakeRequest(), redis_client, asset_id)
    try:
        async for chunk in gen:
            if chunk.startswith("data: "):
                return json.loads(chunk[len("data: ") : -2])  # strip trailing "\n\n"
    finally:
        await gen.aclose()
    raise AssertionError("stream ended without a data line")


async def test_a_stored_event_is_published_and_received_over_sse(pg_pool, redis_client):
    task = asyncio.ensure_future(_first_event(redis_client, asset_id=None))
    await asyncio.sleep(SUBSCRIBE_SETTLE_SECONDS)

    await _store_via_worker(pg_pool, redis_client, asset_id="drone-01", seq=42)

    received = await asyncio.wait_for(task, timeout=READ_TIMEOUT_SECONDS)
    assert received["asset_id"] == "drone-01"
    assert received["seq"] == 42
    assert received["outcome"] == "stored_and_advanced"


async def test_asset_id_filter_only_forwards_the_matching_asset(pg_pool, redis_client):
    task = asyncio.ensure_future(_first_event(redis_client, asset_id="rover-01"))
    await asyncio.sleep(SUBSCRIBE_SETTLE_SECONDS)

    # A different asset first — must NOT be what the filtered subscriber
    # receives, or filtering isn't actually doing anything.
    await _store_via_worker(pg_pool, redis_client, asset_id="drone-01", seq=1)
    await _store_via_worker(pg_pool, redis_client, asset_id="rover-01", seq=1)

    received = await asyncio.wait_for(task, timeout=READ_TIMEOUT_SECONDS)
    assert received["asset_id"] == "rover-01"


async def test_stale_event_is_still_published_with_its_real_outcome(pg_pool, redis_client):
    await _store_via_worker(pg_pool, redis_client, asset_id="drone-02", seq=10)

    task = asyncio.ensure_future(_first_event(redis_client, asset_id="drone-02"))
    await asyncio.sleep(SUBSCRIBE_SETTLE_SECONDS)

    # seq=5 after seq=10 is out-of-order/stale (docs/PLAN.md §4): stored
    # for history, but asset_state doesn't move backwards. The live
    # stream should say exactly that, not claim it advanced anything.
    await _store_via_worker(pg_pool, redis_client, asset_id="drone-02", seq=5)

    received = await asyncio.wait_for(task, timeout=READ_TIMEOUT_SECONDS)
    assert received["seq"] == 5
    assert received["outcome"] == "stored_but_stale"
