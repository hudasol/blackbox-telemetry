"""Integration tests for the query API and health endpoints.

Query tests run against real Postgres, seeded through the worker's own
write path (worker.db.store_event) so the data shape matches exactly
what a real run would produce — not hand-crafted rows that might drift
from what the worker actually writes. Health tests deliberately point
one dependency at an unreachable address to prove /health/ready actually
detects that, and /health/live doesn't (docs/PLAN.md: liveness vs.
readiness are different questions).
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import asyncpg
import httpx
import pytest_asyncio
from app.config import Settings
from app.main import create_app
from redis.asyncio import Redis as AsyncRedis
from telemetry_contract import TelemetryEvent
from worker.db import store_event

REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:16379/0")
POSTGRES_TEST_DSN = os.environ.get(
    "POSTGRES_TEST_DSN", "postgresql://blackbox:blackbox@localhost:5432/blackbox_test"
)
UNREACHABLE_REDIS_URL = "redis://localhost:1/0"  # nothing listens on port 1
UNREACHABLE_POSTGRES_DSN = "postgresql://blackbox:blackbox@localhost:1/blackbox"

ASSET_OFFLINE_TIMEOUT_SECONDS = 5.0


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


@asynccontextmanager
async def _client_for(redis_url: str, postgres_dsn: str, pg_pool: asyncpg.Pool | None = None):
    """Build a client against a fully-wired app, without going through
    the real ASGI lifespan protocol (httpx.ASGITransport doesn't drive
    it) — set the same app.state pieces create_app()'s lifespan would.

    pg_pool lets query tests reuse the exact pool they seeded data
    through; health tests instead pass None so a fresh (possibly
    unreachable) pool gets created from postgres_dsn."""
    settings = Settings(
        redis_url=redis_url,
        postgres_dsn=postgres_dsn,
        log_level="WARNING",
        asset_offline_timeout_seconds=ASSET_OFFLINE_TIMEOUT_SECONDS,
    )
    app = create_app(settings)
    app.state.settings = settings
    app.state.redis = AsyncRedis.from_url(redis_url, decode_responses=True)
    owns_pool = pg_pool is None
    app.state.pg_pool = pg_pool or await asyncpg.create_pool(
        postgres_dsn, min_size=0, max_size=5
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client
    finally:
        await app.state.redis.aclose()
        if owns_pool:
            await app.state.pg_pool.close()


@pytest_asyncio.fixture()
async def api_client(pg_pool):
    async with _client_for(REDIS_TEST_URL, POSTGRES_TEST_DSN, pg_pool=pg_pool) as client:
        yield client


# --- query endpoints -------------------------------------------------


async def test_list_assets_returns_all_known_assets_with_online_status(
    api_client, pg_pool
):
    now = datetime.now(timezone.utc)
    await store_event(pg_pool, make_event(asset_id="drone-01", seq=1), now)
    await store_event(
        pg_pool,
        make_event(asset_id="rover-01", asset_type="rover", seq=1),
        now - timedelta(seconds=100),  # well past the 5s offline timeout
    )

    response = await api_client.get("/api/v1/assets")

    assert response.status_code == 200
    assets = {a["asset_id"]: a for a in response.json()["assets"]}
    assert set(assets) == {"drone-01", "rover-01"}
    assert assets["drone-01"]["online"] is True
    assert assets["rover-01"]["online"] is False


async def test_get_asset_latest_returns_the_current_snapshot(api_client, pg_pool):
    now = datetime.now(timezone.utc)
    await store_event(pg_pool, make_event(seq=1, battery_pct=90), now)
    await store_event(pg_pool, make_event(seq=2, battery_pct=70), now)

    response = await api_client.get("/api/v1/assets/drone-01/latest")

    assert response.status_code == 200
    body = response.json()
    assert body["asset_id"] == "drone-01"
    assert body["latest_seq"] == 2
    assert body["battery_pct"] == 70
    assert body["online"] is True
    assert body["position"] == {"x_m": 1.0, "y_m": 2.0, "z_m": 3.0}


async def test_get_asset_latest_404s_for_an_unknown_asset(api_client):
    response = await api_client.get("/api/v1/assets/does-not-exist/latest")

    assert response.status_code == 404
    assert response.json()["error"] == "asset_not_found"


async def test_get_asset_history_is_bounded_and_ordered_newest_first(api_client, pg_pool):
    now = datetime.now(timezone.utc)
    for seq in range(1, 6):
        await store_event(pg_pool, make_event(seq=seq), now)

    response = await api_client.get("/api/v1/assets/drone-01/history?limit=3")

    assert response.status_code == 200
    body = response.json()
    assert body["limit"] == 3
    assert body["count"] == 3
    assert [e["seq"] for e in body["events"]] == [5, 4, 3]  # newest first


async def test_get_asset_history_for_an_unknown_asset_is_an_empty_200_not_a_404(
    api_client,
):
    response = await api_client.get("/api/v1/assets/never-seen/history")

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "asset_id": "never-seen",
        "limit": 50,
        "from": None,
        "to": None,
        "count": 0,
        "events": [],
    }


async def test_history_limit_is_validated(api_client, pg_pool):
    await store_event(pg_pool, make_event(seq=1), datetime.now(timezone.utc))

    too_low = await api_client.get("/api/v1/assets/drone-01/history?limit=0")
    too_high = await api_client.get("/api/v1/assets/drone-01/history?limit=99999")

    assert too_low.status_code == 422
    assert too_high.status_code == 422
    # Regression: the 422 handler is global and used to hard-code
    # "invalid_telemetry_event" for every validation failure, as if the
    # only thing that could ever 422 was a bad ingest body. A bad query
    # param (nothing to do with a telemetry event) got the same
    # misleading label until this was caught — see docs/PROCESS_LOG.md.
    assert too_low.json()["error"] == "invalid_request"
    assert too_high.json()["error"] == "invalid_request"


async def test_history_preserves_extra_fields(api_client, pg_pool):
    await store_event(
        pg_pool,
        make_event(seq=1, firmware_version="9.9.9"),
        datetime.now(timezone.utc),
    )

    response = await api_client.get("/api/v1/assets/drone-01/history")

    assert response.json()["events"][0]["extra_fields"] == {"firmware_version": "9.9.9"}


# --- history: from/to time-range filtering ------------------------------
#
# Fixed, far-in-the-past anchor rather than datetime.now() for every event
# here — makes the boundary math exact and deterministic instead of racing
# against wall-clock time, and it's safely outside the contract's 60s
# future-clock-skew limit (models.py) regardless of when the test runs.

_ANCHOR = datetime(2026, 1, 1, tzinfo=timezone.utc)


async def _seed_five_minutely_events(pg_pool):
    """seq 1..5 at _ANCHOR, _ANCHOR+1min, ..., _ANCHOR+4min."""
    for seq in range(1, 6):
        await store_event(
            pg_pool,
            make_event(seq=seq, timestamp_utc=_ANCHOR + timedelta(minutes=seq - 1)),
            datetime.now(timezone.utc),
        )


async def test_history_from_and_to_bound_the_range_inclusively(api_client, pg_pool):
    await _seed_five_minutely_events(pg_pool)
    from_ = _ANCHOR + timedelta(minutes=1)  # seq 2, on the boundary
    to = _ANCHOR + timedelta(minutes=3)  # seq 4, on the boundary

    # params= (not a hand-built query string) so httpx percent-encodes the
    # "+" in a UTC offset like "+00:00" — a raw f-string URL leaves it as a
    # literal "+", which a query string decodes as a space, silently
    # corrupting the timestamp into something that fails to parse. Caught
    # by this test actually failing with a 422 the first time it was
    # written with a hand-built URL — see docs/PROCESS_LOG.md.
    response = await api_client.get(
        "/api/v1/assets/drone-01/history",
        params={"from": from_.isoformat(), "to": to.isoformat()},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 3
    assert [e["seq"] for e in body["events"]] == [4, 3, 2]  # newest first, both ends included


async def test_history_from_only_is_open_ended_going_forward(api_client, pg_pool):
    await _seed_five_minutely_events(pg_pool)
    from_ = _ANCHOR + timedelta(minutes=2)

    response = await api_client.get(
        "/api/v1/assets/drone-01/history", params={"from": from_.isoformat()}
    )

    assert [e["seq"] for e in response.json()["events"]] == [5, 4, 3]


async def test_history_to_only_is_open_ended_going_backward(api_client, pg_pool):
    await _seed_five_minutely_events(pg_pool)
    to = _ANCHOR + timedelta(minutes=2)

    response = await api_client.get(
        "/api/v1/assets/drone-01/history", params={"to": to.isoformat()}
    )

    assert [e["seq"] for e in response.json()["events"]] == [3, 2, 1]


async def test_history_range_still_respects_limit(api_client, pg_pool):
    await _seed_five_minutely_events(pg_pool)
    from_ = _ANCHOR
    to = _ANCHOR + timedelta(minutes=4)  # covers all 5

    response = await api_client.get(
        "/api/v1/assets/drone-01/history",
        params={"from": from_.isoformat(), "to": to.isoformat(), "limit": 2},
    )

    body = response.json()
    assert body["count"] == 2
    assert [e["seq"] for e in body["events"]] == [5, 4]  # still newest-first, still bounded


async def test_history_naive_from_to_are_treated_as_utc(api_client, pg_pool):
    # Same rule as the ingest contract's timestamp_utc (telemetry_contract
    # models.py): no offset in the string means UTC, not "guess and hope".
    await _seed_five_minutely_events(pg_pool)
    naive_from = (_ANCHOR + timedelta(minutes=1)).replace(tzinfo=None)
    naive_to = (_ANCHOR + timedelta(minutes=3)).replace(tzinfo=None)

    response = await api_client.get(
        "/api/v1/assets/drone-01/history",
        params={"from": naive_from.isoformat(), "to": naive_to.isoformat()},
    )

    assert [e["seq"] for e in response.json()["events"]] == [4, 3, 2]


async def test_history_from_after_to_is_422(api_client, pg_pool):
    await _seed_five_minutely_events(pg_pool)
    from_ = _ANCHOR + timedelta(minutes=3)
    to = _ANCHOR + timedelta(minutes=1)

    response = await api_client.get(
        "/api/v1/assets/drone-01/history",
        params={"from": from_.isoformat(), "to": to.isoformat()},
    )

    assert response.status_code == 422
    assert response.json()["error"] == "invalid_request"


async def test_history_from_to_echoed_back_in_the_response(api_client, pg_pool):
    await _seed_five_minutely_events(pg_pool)
    from_ = _ANCHOR + timedelta(minutes=1)
    to = _ANCHOR + timedelta(minutes=3)

    response = await api_client.get(
        "/api/v1/assets/drone-01/history",
        params={"from": from_.isoformat(), "to": to.isoformat()},
    )

    body = response.json()
    assert datetime.fromisoformat(body["from"]) == from_
    assert datetime.fromisoformat(body["to"]) == to


# --- query endpoints, dependency unavailable ---------------------------
#
# Distinct from the health-endpoint 503 tests below: these prove each
# query endpoint's own _query_unavailable() branch (docs/PLAN.md,
# failure policy — mirrors ingest's "Redis unavailable" -> 503, just for
# query's Postgres dependency instead), not just that /health/ready can
# detect the same outage. min_size=0 means pool creation itself always
# succeeds even when Postgres is down (docs/PROCESS_LOG.md, Query API
# entry) — the failure only surfaces on the first real acquire, which is
# exactly what these exercise.


async def test_list_assets_is_503_when_postgres_is_unreachable():
    async with _client_for(REDIS_TEST_URL, UNREACHABLE_POSTGRES_DSN) as client:
        response = await client.get("/api/v1/assets")

    assert response.status_code == 503
    assert response.json()["error"] == "query_unavailable"


async def test_get_asset_latest_is_503_when_postgres_is_unreachable():
    async with _client_for(REDIS_TEST_URL, UNREACHABLE_POSTGRES_DSN) as client:
        response = await client.get("/api/v1/assets/drone-01/latest")

    assert response.status_code == 503
    assert response.json()["error"] == "query_unavailable"


async def test_get_asset_history_is_503_when_postgres_is_unreachable():
    async with _client_for(REDIS_TEST_URL, UNREACHABLE_POSTGRES_DSN) as client:
        response = await client.get("/api/v1/assets/drone-01/history")

    assert response.status_code == 503
    assert response.json()["error"] == "query_unavailable"


# --- health endpoints --------------------------------------------------


async def test_health_live_is_always_200_even_with_both_dependencies_down():
    async with _client_for(UNREACHABLE_REDIS_URL, UNREACHABLE_POSTGRES_DSN) as client:
        response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


async def test_health_ready_is_200_when_both_dependencies_are_reachable():
    async with _client_for(REDIS_TEST_URL, POSTGRES_TEST_DSN) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["checks"] == {"redis": "ok", "postgres": "ok"}


async def test_health_ready_is_503_when_postgres_is_unreachable():
    async with _client_for(REDIS_TEST_URL, UNREACHABLE_POSTGRES_DSN) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    checks = response.json()["checks"]
    assert checks["redis"] == "ok"
    assert checks["postgres"].startswith("error")


async def test_health_ready_is_503_when_redis_is_unreachable():
    async with _client_for(UNREACHABLE_REDIS_URL, POSTGRES_TEST_DSN) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    checks = response.json()["checks"]
    assert checks["redis"].startswith("error")
    assert checks["postgres"] == "ok"
