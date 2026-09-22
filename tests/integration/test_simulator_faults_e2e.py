"""Integration test: simulator scheduling + HTTP client against a real
(in-process) ingest API and a real Redis, proving each of the four fault
switches produces the actual wire behavior it's meant to (docs/PLAN.md,
failure policy) — not just that the pure scheduler logic looks right in
isolation (tests/unit/test_simulator_scheduler.py covers that).

Drives plan_tick()/send_plan() by hand rather than running simulator.main's
real loop, so this stays fast and deterministic instead of waiting on
sleeps.
"""

from __future__ import annotations

import os
import random

import httpx
import pytest_asyncio
import redis as redis_sync
from app.config import Settings
from app.main import create_app
from redis.asyncio import Redis as AsyncRedis
from telemetry_contract import STREAM_NAME

from simulator.assets import SimulatedAsset
from simulator.faults import FaultInjector, FaultRates
from simulator.main import send_plan
from simulator.scheduler import plan_tick

REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:16379/0")


@pytest_asyncio.fixture()
async def redis_conn():
    conn = redis_sync.Redis.from_url(REDIS_TEST_URL, decode_responses=True)
    conn.flushdb()
    yield conn
    conn.flushdb()
    conn.close()


@pytest_asyncio.fixture()
async def api_client(redis_conn):
    app = create_app(Settings(redis_url=REDIS_TEST_URL, log_level="WARNING"))
    # Set up what the app's own lifespan would (app.state.redis) directly,
    # rather than driving the ASGI lifespan protocol through the test
    # transport — simpler, and exercises the exact same handler code path.
    app.state.redis = AsyncRedis.from_url(REDIS_TEST_URL, decode_responses=True)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client
    await app.state.redis.aclose()


async def test_duplicate_switch_sends_the_same_event_twice(api_client, redis_conn):
    asset = SimulatedAsset(asset_id="drone-01", asset_type="drone", rng=random.Random(1))
    injector = FaultInjector(FaultRates(duplicate=1.0), random.Random(2))

    plan = plan_tick(asset, injector, held=None)
    statuses = await send_plan(api_client, "", plan, injector)

    assert statuses == [202, 202]
    assert redis_conn.xlen(STREAM_NAME) == 2
    entries = redis_conn.xrange(STREAM_NAME)
    assert entries[0][1]["seq"] == entries[1][1]["seq"] == "1"


async def test_bad_data_switch_gets_rejected_and_never_enqueued(api_client, redis_conn):
    asset = SimulatedAsset(asset_id="drone-01", asset_type="drone", rng=random.Random(1))
    injector = FaultInjector(FaultRates(bad_data=1.0), random.Random(2))

    plan = plan_tick(asset, injector, held=None)
    statuses = await send_plan(api_client, "", plan, injector)

    assert statuses == [422]
    assert redis_conn.xlen(STREAM_NAME) == 0


async def test_out_of_order_switch_delivers_a_lower_seq_after_a_higher_one(
    api_client, redis_conn
):
    asset = SimulatedAsset(asset_id="drone-01", asset_type="drone", rng=random.Random(1))
    injector = FaultInjector(FaultRates(out_of_order=1.0), random.Random(2))

    first_plan = plan_tick(asset, injector, held=None)
    assert first_plan.to_send == []  # held back this tick
    await send_plan(api_client, "", first_plan, injector)

    second_plan = plan_tick(asset, injector, held=first_plan.held)
    statuses = await send_plan(api_client, "", second_plan, injector)

    assert statuses == [202, 202]
    entries = redis_conn.xrange(STREAM_NAME)
    seqs_on_the_wire = [int(fields["seq"]) for _, fields in entries]
    assert seqs_on_the_wire == [2, 1]  # arrived out of order, exactly as intended


async def test_silence_switch_sends_nothing_over_the_wire(api_client, redis_conn):
    asset = SimulatedAsset(asset_id="drone-01", asset_type="drone", rng=random.Random(1))
    injector = FaultInjector(FaultRates(), random.Random(2))

    plan = plan_tick(asset, injector, held=None, force_silent=True)
    statuses = await send_plan(api_client, "", plan, injector)

    assert statuses == []
    assert redis_conn.xlen(STREAM_NAME) == 0
