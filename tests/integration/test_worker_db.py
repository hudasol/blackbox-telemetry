"""Integration tests for worker.db against a real Postgres.

These pin down the two guarantees the whole task hinges on: the same
logical event never becomes two rows, and current state never regresses
(docs/PLAN.md, failure policy).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from telemetry_contract import TelemetryEvent
from worker.db import store_event


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


async def test_fresh_event_is_inserted_and_advances_state(pg_pool):
    event = make_event(seq=1)
    result = await store_event(pg_pool, event, datetime.now(timezone.utc))

    assert result.event_inserted is True
    assert result.state_advanced is True
    assert result.outcome == "stored_and_advanced"

    row = await pg_pool.fetchrow(
        "SELECT * FROM telemetry_events WHERE asset_id = $1 AND seq = $2",
        "drone-01",
        1,
    )
    assert row is not None
    state = await pg_pool.fetchrow(
        "SELECT * FROM asset_state WHERE asset_id = $1", "drone-01"
    )
    assert state["latest_seq"] == 1


async def test_duplicate_seq_is_not_inserted_twice(pg_pool):
    event = make_event(seq=5)
    await store_event(pg_pool, event, datetime.now(timezone.utc))
    result = await store_event(pg_pool, event, datetime.now(timezone.utc))

    assert result.event_inserted is False
    assert result.outcome == "duplicate_ignored"

    count = await pg_pool.fetchval(
        "SELECT count(*) FROM telemetry_events WHERE asset_id = $1 AND seq = $2",
        "drone-01",
        5,
    )
    assert count == 1


async def test_out_of_order_event_is_stored_but_does_not_regress_state(pg_pool):
    newer = make_event(seq=10, battery_pct=50)
    older = make_event(seq=3, battery_pct=99)

    await store_event(pg_pool, newer, datetime.now(timezone.utc))
    result = await store_event(pg_pool, older, datetime.now(timezone.utc))

    assert result.event_inserted is True  # stored to history
    assert result.state_advanced is False  # but didn't move state backwards
    assert result.outcome == "stored_but_stale"

    # Both rows exist in history...
    seqs = {
        r["seq"]
        for r in await pg_pool.fetch(
            "SELECT seq FROM telemetry_events WHERE asset_id = $1", "drone-01"
        )
    }
    assert seqs == {10, 3}

    # ...but current state is still the newer one.
    state = await pg_pool.fetchrow(
        "SELECT * FROM asset_state WHERE asset_id = $1", "drone-01"
    )
    assert state["latest_seq"] == 10
    assert state["battery_pct"] == 50


async def test_state_advances_when_a_genuinely_newer_seq_arrives(pg_pool):
    await store_event(pg_pool, make_event(seq=1, battery_pct=90), datetime.now(timezone.utc))
    result = await store_event(
        pg_pool, make_event(seq=2, battery_pct=80), datetime.now(timezone.utc)
    )

    assert result.outcome == "stored_and_advanced"
    state = await pg_pool.fetchrow(
        "SELECT * FROM asset_state WHERE asset_id = $1", "drone-01"
    )
    assert state["latest_seq"] == 2
    assert state["battery_pct"] == 80


async def test_equal_seq_after_being_stored_counts_as_duplicate_not_regression(pg_pool):
    event = make_event(seq=7)
    await store_event(pg_pool, event, datetime.now(timezone.utc))
    result = await store_event(pg_pool, event, datetime.now(timezone.utc))

    # Same (asset_id, seq) twice: the unique constraint catches it before
    # the state-advance question even comes up.
    assert result.outcome == "duplicate_ignored"


async def test_extra_fields_are_preserved_in_history(pg_pool):
    event = make_event(seq=1, firmware_version="9.9.9")
    await store_event(pg_pool, event, datetime.now(timezone.utc))

    row = await pg_pool.fetchrow(
        "SELECT extra_fields FROM telemetry_events WHERE asset_id = $1 AND seq = $2",
        "drone-01",
        1,
    )
    import json

    assert json.loads(row["extra_fields"]) == {"firmware_version": "9.9.9"}


async def test_different_assets_are_independent(pg_pool):
    await store_event(
        pg_pool, make_event(asset_id="rover-01", seq=1), datetime.now(timezone.utc)
    )
    await store_event(
        pg_pool, make_event(asset_id="quad-01", seq=1), datetime.now(timezone.utc)
    )

    assets = {
        r["asset_id"] for r in await pg_pool.fetch("SELECT asset_id FROM asset_state")
    }
    assert assets == {"rover-01", "quad-01"}


async def test_a_newer_but_out_of_order_asset_sequence_never_flickers_backwards(pg_pool):
    # Simulates events arriving out of order: 1, 3, 2 — state should end
    # up at 3, never dip back to 2 after already having seen 3.
    base_time = datetime.now(timezone.utc)
    await store_event(pg_pool, make_event(seq=1), base_time)
    await store_event(pg_pool, make_event(seq=3), base_time + timedelta(seconds=2))
    result = await store_event(pg_pool, make_event(seq=2), base_time + timedelta(seconds=1))

    assert result.state_advanced is False
    state = await pg_pool.fetchrow(
        "SELECT latest_seq FROM asset_state WHERE asset_id = $1", "drone-01"
    )
    assert state["latest_seq"] == 3
