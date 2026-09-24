"""Integration tests for the Prometheus metrics added in
docs/IMPROVEMENTS.md, "real observability" — against real Redis and
real Postgres, same conventions as the rest of tests/integration.

These don't assert on absolute counter values: prometheus_client's
Counter/Gauge objects are process-global (module-level singletons), so
every test in this file (and every other test file that happens to
exercise ingest or the worker in the same pytest run) adds to the same
totals. Each test instead reads the value immediately before and after
its own action and asserts the *delta* — the only thing a shared-state
metric can honestly promise.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import httpx
import pytest_asyncio
from app.config import Settings
from app.main import create_app
from prometheus_client import REGISTRY
from redis.asyncio import Redis as AsyncRedis
from telemetry_contract import TelemetryEvent
from worker.processing import process_message
from worker.redis_stream import ensure_consumer_group, stream_lag

REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:16379/0")
POSTGRES_TEST_DSN = os.environ.get(
    "POSTGRES_TEST_DSN", "postgresql://blackbox:blackbox@localhost:5432/blackbox_test"
)


def _sample(name: str, labels: dict | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


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


async def test_metrics_endpoint_exposes_prometheus_text_format(api_client):
    response = await api_client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    # Both metric families are always present (even at zero) once the
    # process has imported app.metrics — prometheus_client registers a
    # metric at definition time, not on first use.
    assert "blackbox_ingest_events_total" in response.text
    assert "blackbox_ingest_latency_seconds" in response.text


async def test_accepted_ingest_increments_the_accepted_counter(api_client):
    before = _sample("blackbox_ingest_events_total", {"outcome": "accepted"})

    response = await api_client.post("/api/v1/telemetry", json=valid_payload(seq=101))

    assert response.status_code == 202
    after = _sample("blackbox_ingest_events_total", {"outcome": "accepted"})
    assert after == before + 1


async def test_rejected_ingest_increments_the_rejected_counter_not_accepted(api_client):
    accepted_before = _sample("blackbox_ingest_events_total", {"outcome": "accepted"})
    rejected_before = _sample("blackbox_ingest_events_total", {"outcome": "rejected"})

    response = await api_client.post(
        "/api/v1/telemetry", json=valid_payload(battery_pct=500)  # out of 0-100 range
    )

    assert response.status_code == 422
    assert _sample("blackbox_ingest_events_total", {"outcome": "accepted"}) == accepted_before
    assert _sample("blackbox_ingest_events_total", {"outcome": "rejected"}) == rejected_before + 1


async def test_worker_records_outcome_and_latency_for_a_stored_event(pg_pool, redis_client):
    stored_before = _sample(
        "blackbox_worker_messages_processed_total", {"outcome": "stored_and_advanced"}
    )
    latency_count_before = _sample("blackbox_worker_processing_latency_seconds_count")

    await ensure_consumer_group(redis_client)
    event = TelemetryEvent(**valid_payload(seq=55))
    message_id = await redis_client.xadd(
        "telemetry.raw",
        {
            "asset_id": event.asset_id,
            "seq": str(event.seq),
            "received_at": datetime.now(timezone.utc).isoformat(),
            "payload": event.model_dump_json(),
        },
    )
    response = await redis_client.xreadgroup(
        "worker-group", "metrics-test-consumer", {"telemetry.raw": ">"}, count=1
    )
    _stream, messages = response[0]
    read_id, fields = messages[0]
    assert read_id == message_id

    await process_message(redis_client, pg_pool, read_id, fields, asyncio.Event())

    assert (
        _sample("blackbox_worker_messages_processed_total", {"outcome": "stored_and_advanced"})
        == stored_before + 1
    )
    assert _sample("blackbox_worker_processing_latency_seconds_count") == latency_count_before + 1


async def test_worker_records_deadlettered_outcome_for_a_poison_message(pg_pool, redis_client):
    deadlettered_before = _sample(
        "blackbox_worker_messages_processed_total", {"outcome": "deadlettered"}
    )

    await ensure_consumer_group(redis_client)
    await redis_client.xadd("telemetry.raw", {"payload": "not valid json"})
    response = await redis_client.xreadgroup(
        "worker-group", "metrics-test-consumer-2", {"telemetry.raw": ">"}, count=1
    )
    _stream, messages = response[0]
    message_id, fields = messages[0]

    await process_message(redis_client, pg_pool, message_id, fields, asyncio.Event())

    assert (
        _sample("blackbox_worker_messages_processed_total", {"outcome": "deadlettered"})
        == deadlettered_before + 1
    )


async def test_stream_lag_reflects_undelivered_backlog_then_drains_to_zero(redis_client):
    await ensure_consumer_group(redis_client)
    assert await stream_lag(redis_client) == 0  # freshly created group, nothing sent yet

    for seq in range(3):
        await redis_client.xadd("telemetry.raw", {"payload": f"msg-{seq}"})

    # Not yet delivered to any consumer in the group — this is exactly
    # what "backlog" means here, distinct from "pending" (delivered but
    # unacked), which XPENDING already covers elsewhere in this codebase.
    assert await stream_lag(redis_client) == 3

    response = await redis_client.xreadgroup(
        "worker-group", "lag-test-consumer", {"telemetry.raw": ">"}, count=3
    )
    _stream, messages = response[0]
    for message_id, _fields in messages:
        await redis_client.xack("telemetry.raw", "worker-group", message_id)

    assert await stream_lag(redis_client) == 0
