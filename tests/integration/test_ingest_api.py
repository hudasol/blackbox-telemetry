"""Integration tests for POST /api/v1/telemetry.

Runs against a real Redis (no mocking the queue — that's the one dependency
this endpoint actually talks to). Requires REDIS_TEST_URL to point at a
throwaway Redis instance; see docs/PLAN.md for why ingest only depends on
Redis, never Postgres.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest
import redis as redis_sync
from app.config import Settings
from app.main import create_app
from fastapi.testclient import TestClient
from telemetry_contract import STREAM_NAME

REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:16379/0")
UNREACHABLE_REDIS_URL = "redis://localhost:1/0"  # nothing listens on port 1


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


@pytest.fixture()
def redis_conn():
    conn = redis_sync.Redis.from_url(REDIS_TEST_URL, decode_responses=True)
    conn.flushdb()
    yield conn
    conn.flushdb()
    conn.close()


@pytest.fixture()
def client(redis_conn):
    app = create_app(Settings(redis_url=REDIS_TEST_URL, log_level="WARNING"))
    with TestClient(app) as c:
        yield c


def test_valid_event_is_accepted_and_enqueued(client, redis_conn):
    response = client.post("/api/v1/telemetry", json=valid_payload())

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["asset_id"] == "drone-01"
    assert body["seq"] == 1
    assert "stream_id" in body

    assert redis_conn.xlen(STREAM_NAME) == 1
    entries = redis_conn.xrange(STREAM_NAME)
    _, fields = entries[0]
    assert fields["asset_id"] == "drone-01"
    assert fields["seq"] == "1"
    assert "received_at" in fields
    assert '"asset_id":"drone-01"' in fields["payload"]


def test_missing_required_field_is_rejected_and_not_enqueued(client, redis_conn):
    payload = valid_payload()
    del payload["battery_pct"]

    response = client.post("/api/v1/telemetry", json=payload)

    assert response.status_code == 422
    assert response.json()["error"] == "invalid_telemetry_event"
    assert redis_conn.xlen(STREAM_NAME) == 0


def test_unknown_enum_value_is_rejected_and_not_enqueued(client, redis_conn):
    response = client.post(
        "/api/v1/telemetry", json=valid_payload(asset_type="submarine")
    )

    assert response.status_code == 422
    assert redis_conn.xlen(STREAM_NAME) == 0


def test_out_of_range_value_is_rejected_and_not_enqueued(client, redis_conn):
    response = client.post("/api/v1/telemetry", json=valid_payload(battery_pct=150))

    assert response.status_code == 422
    assert redis_conn.xlen(STREAM_NAME) == 0


def test_negative_seq_is_rejected_and_not_enqueued(client, redis_conn):
    response = client.post("/api/v1/telemetry", json=valid_payload(seq=-1))

    assert response.status_code == 422
    assert redis_conn.xlen(STREAM_NAME) == 0


def test_future_clock_skew_is_rejected_cleanly_not_a_500(client, redis_conn):
    # Regression test: this specific rejection comes from a @model_validator
    # raising ValueError, whose pydantic error carries the raw exception
    # under ctx.error — that crashed the handler with a non-JSON-serializable
    # response (TypeError) before the handler stripped ctx. A field-type
    # error (e.g. missing/out-of-range) never hit this path, which is why
    # it wasn't caught by the other rejection tests above.
    from datetime import timedelta

    too_far = datetime.now(timezone.utc) + timedelta(hours=2)
    response = client.post(
        "/api/v1/telemetry", json=valid_payload(timestamp_utc=too_far.isoformat())
    )

    assert response.status_code == 422
    assert response.json()["error"] == "invalid_telemetry_event"
    assert redis_conn.xlen(STREAM_NAME) == 0


def test_duplicate_event_is_enqueued_both_times(client, redis_conn):
    # Deduplication is the worker/Postgres's job, not ingest's (docs/PLAN.md,
    # decision D2) — the API enqueues the same (asset_id, seq) twice without
    # complaint.
    payload = valid_payload(seq=7)

    first = client.post("/api/v1/telemetry", json=payload)
    second = client.post("/api/v1/telemetry", json=payload)

    assert first.status_code == 202
    assert second.status_code == 202
    assert redis_conn.xlen(STREAM_NAME) == 2


def test_oversized_body_is_rejected_with_413_and_never_enqueued(redis_conn):
    # extra="allow" (telemetry_contract/models.py) means a big padding
    # field is a perfectly valid TelemetryEvent by shape — nothing in
    # the model itself would ever reject this. Only the size middleware
    # in front of it does, and it must do so before anything reaches
    # Redis (docs/PLAN.md, failure policy: never pretend a rejected
    # request was accepted).
    app = create_app(
        Settings(redis_url=REDIS_TEST_URL, log_level="WARNING", max_request_body_bytes=256)
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/telemetry", json=valid_payload(padding="x" * 2000)
        )

    assert response.status_code == 413
    assert response.json()["error"] == "payload_too_large"
    assert redis_conn.xlen(STREAM_NAME) == 0


def test_body_within_the_configured_limit_is_still_accepted(redis_conn):
    # A real (unpadded) event is a few hundred bytes — 2048 leaves
    # comfortable room for it without approaching the 2000-byte padding
    # used above to trip the limit.
    app = create_app(
        Settings(redis_url=REDIS_TEST_URL, log_level="WARNING", max_request_body_bytes=2048)
    )
    with TestClient(app) as client:
        response = client.post("/api/v1/telemetry", json=valid_payload())

    assert response.status_code == 202
    assert redis_conn.xlen(STREAM_NAME) == 1


def test_redis_unavailable_returns_503_and_never_pretends_success(redis_conn):
    app = create_app(Settings(redis_url=UNREACHABLE_REDIS_URL, log_level="WARNING"))
    with TestClient(app) as client:
        response = client.post("/api/v1/telemetry", json=valid_payload())

    assert response.status_code == 503
    assert response.json()["error"] == "queue_unavailable"
    # The real stream (a different Redis entirely) was never touched.
    assert redis_conn.xlen(STREAM_NAME) == 0
