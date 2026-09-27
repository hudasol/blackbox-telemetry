"""Integration tests for GET/POST .../deadletter — against a real app and
real Redis, same pattern as test_query_api.py's _client_for helper.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import httpx
import pytest
import pytest_asyncio
from app.config import Settings
from app.main import create_app
from prometheus_client import REGISTRY
from redis.asyncio import Redis as AsyncRedis
from telemetry_contract import DEADLETTER_STREAM_NAME, STREAM_NAME

REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:16379/0")
POSTGRES_TEST_DSN = os.environ.get(
    "POSTGRES_TEST_DSN", "postgresql://blackbox:blackbox@localhost:5432/blackbox_test"
)


@asynccontextmanager
async def _client_for(redis_url: str, postgres_dsn: str, pg_pool: asyncpg.Pool):
    settings = Settings(redis_url=redis_url, postgres_dsn=postgres_dsn, log_level="WARNING")
    app = create_app(settings)
    app.state.settings = settings
    app.state.redis = AsyncRedis.from_url(redis_url, decode_responses=True)
    app.state.pg_pool = pg_pool
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client
    finally:
        await app.state.redis.aclose()


@pytest_asyncio.fixture()
async def api_client(pg_pool):
    async with _client_for(REDIS_TEST_URL, POSTGRES_TEST_DSN, pg_pool=pg_pool) as client:
        yield client


async def _add_deadletter_entry(redis_client, **field_overrides: str) -> str:
    fields = {
        "original_message_id": "1234-0",
        "original_asset_id": "drone-01",
        "original_seq": "5",
        "payload": "not valid json",
        "received_at": datetime.now(timezone.utc).isoformat(),
        "reason": "Expecting value: line 1 column 1 (char 0)",
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    fields.update(field_overrides)
    return await redis_client.xadd(DEADLETTER_STREAM_NAME, fields)


async def test_empty_deadletter_stream_lists_nothing(api_client):
    response = await api_client.get("/api/v1/deadletter")
    assert response.status_code == 200
    assert response.json() == {"count": 0, "entries": []}


async def test_deadletter_entry_is_listed_newest_first(api_client, redis_client):
    first_id = await _add_deadletter_entry(redis_client, original_seq="1")
    second_id = await _add_deadletter_entry(redis_client, original_seq="2")

    response = await api_client.get("/api/v1/deadletter")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    # Newest first, same convention as GET .../history.
    assert [e["deadletter_id"] for e in body["entries"]] == [second_id, first_id]
    assert body["entries"][0]["asset_id"] == "drone-01"
    assert body["entries"][0]["seq"] == "2"
    assert body["entries"][0]["reason"]


async def test_unparseable_payload_is_shown_as_raw_text_not_500(api_client, redis_client):
    # The payload field itself is "not valid json" (that's *why* it's
    # dead-lettered) — listing it must not itself crash trying to parse it.
    await _add_deadletter_entry(redis_client, payload="not valid json")

    response = await api_client.get("/api/v1/deadletter")

    assert response.status_code == 200
    assert response.json()["entries"][0]["payload"] == "not valid json"


async def test_replay_puts_the_message_back_on_telemetry_raw_and_removes_it(
    api_client, redis_client
):
    valid_payload_json = (
        '{"asset_id":"drone-01","asset_type":"drone","seq":9,'
        '"timestamp_utc":"2026-01-01T00:00:00+00:00",'
        '"position":{"x_m":1.0,"y_m":2.0,"z_m":3.0},"speed_mps":1.0,'
        '"heading_deg":10.0,"battery_pct":50.0,"temperature_c":20.0,'
        '"link_quality_pct":90.0,"mode":"idle"}'
    )
    entry_id = await _add_deadletter_entry(
        redis_client, original_seq="9", payload=valid_payload_json
    )

    response = await api_client.post(f"/api/v1/deadletter/{entry_id}/replay")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "replayed"
    assert body["deadletter_id"] == entry_id
    assert "new_message_id" in body

    # Back on telemetry.raw, ready for the worker to pick up like any
    # other message...
    raw_entries = await redis_client.xrange(STREAM_NAME)
    assert len(raw_entries) == 1
    _id, raw_fields = raw_entries[0]
    assert raw_fields["asset_id"] == "drone-01"
    assert raw_fields["seq"] == "9"
    assert raw_fields["payload"] == valid_payload_json

    # ...and no longer sitting in the dead-letter stream, so the list only
    # ever shows what's still actually outstanding.
    assert await redis_client.xrange(DEADLETTER_STREAM_NAME) == []


async def test_replay_increments_the_ingest_events_replayed_counter(api_client, redis_client):
    # The bug this closes: replay's XADD (inside _REPLAY_SCRIPT) put a
    # real message onto telemetry.raw without ever touching
    # blackbox_ingest_events_total — the same metric POST
    # /api/v1/telemetry increments on every accepted event — so a
    # dashboard built on it understated real traffic reaching the queue
    # by however much replay volume there was. Same delta-based
    # assertion style as test_metrics.py, since the counter is a
    # process-global shared across the whole test run.
    before = REGISTRY.get_sample_value(
        "blackbox_ingest_events_total", {"outcome": "replayed"}
    ) or 0.0

    entry_id = await _add_deadletter_entry(redis_client, original_seq="9")
    response = await api_client.post(f"/api/v1/deadletter/{entry_id}/replay")
    assert response.status_code == 200

    after = REGISTRY.get_sample_value(
        "blackbox_ingest_events_total", {"outcome": "replayed"}
    ) or 0.0
    assert after == before + 1


async def test_replay_of_unknown_id_is_404(api_client):
    response = await api_client.post("/api/v1/deadletter/9999999999999-0/replay")
    assert response.status_code == 404
    assert response.json()["error"] == "deadletter_entry_not_found"


@pytest.mark.parametrize(
    "bad_id",
    [
        "not-an-id",
        "12345",  # missing -seq
        "12345-",  # missing seq value
        "-0",  # missing ms value
        "12345-0-0",  # extra segment
        "abc-def",
        "-1-0",  # negative
        "12.5-0",  # not an integer
    ],
)
async def test_replay_of_malformed_id_is_400_not_503(api_client, bad_id):
    """The bug this closes: an id that isn't Redis's <ms>-<seq> stream-id
    shape used to reach XRANGE, which rejects it with a ResponseError —
    a RedisError subclass caught by the same except block as an actual
    Redis outage, so a client typo came back indistinguishable from a
    real 503. It's now checked before Redis is ever touched."""
    response = await api_client.post(f"/api/v1/deadletter/{bad_id}/replay")
    assert response.status_code == 400
    body = response.json()
    assert body["error"] == "invalid_deadletter_id"
    assert body["deadletter_id"] == bad_id


async def test_replay_of_well_formed_but_nonexistent_id_is_still_404(api_client):
    # A malformed id (above) and a well-formed-but-nonexistent one (this
    # test, same as test_replay_of_unknown_id_is_404) are different
    # problems and must stay distinguishable: 400 for "that's not a real
    # id", 404 for "no entry at that id".
    response = await api_client.post("/api/v1/deadletter/123456789-0/replay")
    assert response.status_code == 404
    assert response.json()["error"] == "deadletter_entry_not_found"


async def test_replaying_the_same_id_twice_does_not_duplicate_the_message(
    api_client, redis_client
):
    """The bug this closes: replay used to be XADD-then-XDEL as two
    separate round-trips, so a retried replay request (the client didn't
    see the first response, or just double-clicked) could re-enqueue the
    same dead-lettered message a second time before the first XDEL ever
    removed it. The lookup, XADD, and XDEL are now one atomic script, so
    a second replay of an already-replayed id must see nothing there —
    not a second copy on telemetry.raw."""
    entry_id = await _add_deadletter_entry(redis_client, original_seq="9")

    first = await api_client.post(f"/api/v1/deadletter/{entry_id}/replay")
    assert first.status_code == 200

    second = await api_client.post(f"/api/v1/deadletter/{entry_id}/replay")
    assert second.status_code == 404
    assert second.json()["error"] == "deadletter_entry_not_found"

    # Exactly one copy on telemetry.raw, not two.
    raw_entries = await redis_client.xrange(STREAM_NAME)
    assert len(raw_entries) == 1


async def test_concurrent_replay_of_the_same_id_only_replays_once(
    api_client, redis_client
):
    """Same bug, the race-without-any-blip version: two requests for the
    same deadletter_id arriving at the same time both used to pass the
    XRANGE lookup (it doesn't remove anything) and both XADD — two
    messages on telemetry.raw from one dead-lettered entry. The atomic
    script serializes them on the server: whichever runs first sees the
    entry and moves it, the other sees it already gone."""
    entry_id = await _add_deadletter_entry(redis_client, original_seq="9")

    responses = await asyncio.gather(
        api_client.post(f"/api/v1/deadletter/{entry_id}/replay"),
        api_client.post(f"/api/v1/deadletter/{entry_id}/replay"),
    )
    statuses = sorted(r.status_code for r in responses)
    assert statuses == [200, 404]

    raw_entries = await redis_client.xrange(STREAM_NAME)
    assert len(raw_entries) == 1
    assert await redis_client.xrange(DEADLETTER_STREAM_NAME) == []
