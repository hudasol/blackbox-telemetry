"""Unit tests for the shared telemetry contract (libs/telemetry_contract).

These pin down the validation rules documented in docs/PLAN.md, section 3,
so a change to the contract that silently loosens or tightens a rule shows
up here first.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError
from telemetry_contract import AssetMode, AssetType, TelemetryEvent
from telemetry_contract.constants import MAX_CLOCK_SKEW_SECONDS


def make_payload(**overrides: object) -> dict:
    """A valid event payload, with individual fields overridable."""
    payload = {
        "asset_id": "drone-01",
        "asset_type": "drone",
        "seq": 42,
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


def test_valid_event_parses():
    event = TelemetryEvent(**make_payload())
    assert event.asset_id == "drone-01"
    assert event.asset_type is AssetType.DRONE
    assert event.mode is AssetMode.MOVING
    assert event.idempotency_key == ("drone-01", 42)


def test_missing_required_field_is_rejected():
    payload = make_payload()
    del payload["battery_pct"]
    with pytest.raises(ValidationError):
        TelemetryEvent(**payload)


@pytest.mark.parametrize("bad_seq", [-1, -100])
def test_negative_seq_is_rejected(bad_seq):
    with pytest.raises(ValidationError):
        TelemetryEvent(**make_payload(seq=bad_seq))


def test_seq_zero_is_allowed():
    TelemetryEvent(**make_payload(seq=0))


@pytest.mark.parametrize("bad_pct", [-0.1, 100.1, -50, 500])
def test_battery_pct_out_of_range_is_rejected(bad_pct):
    with pytest.raises(ValidationError):
        TelemetryEvent(**make_payload(battery_pct=bad_pct))


@pytest.mark.parametrize("bad_pct", [-0.1, 100.1])
def test_link_quality_pct_out_of_range_is_rejected(bad_pct):
    with pytest.raises(ValidationError):
        TelemetryEvent(**make_payload(link_quality_pct=bad_pct))


@pytest.mark.parametrize("bad_heading", [-0.1, 360.1, -10, 720])
def test_heading_deg_out_of_range_is_rejected(bad_heading):
    with pytest.raises(ValidationError):
        TelemetryEvent(**make_payload(heading_deg=bad_heading))


def test_boundary_values_are_allowed():
    # 0 and the documented max are valid, not off-by-one rejected.
    TelemetryEvent(**make_payload(battery_pct=0, link_quality_pct=100, heading_deg=0))
    TelemetryEvent(**make_payload(battery_pct=100, link_quality_pct=0, heading_deg=360))


def test_unknown_mode_is_rejected():
    with pytest.raises(ValidationError):
        TelemetryEvent(**make_payload(mode="doing-a-barrel-roll"))


def test_unknown_asset_type_is_rejected():
    with pytest.raises(ValidationError):
        TelemetryEvent(**make_payload(asset_type="submarine"))


@pytest.mark.parametrize("field", ["speed_mps", "temperature_c"])
def test_nan_and_infinity_are_rejected(field):
    with pytest.raises(ValidationError):
        TelemetryEvent(**make_payload(**{field: math.nan}))
    with pytest.raises(ValidationError):
        TelemetryEvent(**make_payload(**{field: math.inf}))


def test_non_finite_position_component_is_rejected():
    payload = make_payload()
    payload["position"] = {"x_m": math.nan, "y_m": 0.0, "z_m": 0.0}
    with pytest.raises(ValidationError):
        TelemetryEvent(**payload)


def test_naive_timestamp_is_treated_as_utc():
    # A naive datetime with no tzinfo, but whose wall-clock value already
    # *is* UTC (not local time — the sandbox itself doesn't run in UTC,
    # which is exactly why this needs to be explicit).
    naive = datetime.now(timezone.utc).replace(tzinfo=None)
    event = TelemetryEvent(**make_payload(timestamp_utc=naive))
    assert event.timestamp_utc.tzinfo is not None


def test_timestamp_far_in_the_future_is_rejected():
    too_far = datetime.now(timezone.utc) + timedelta(
        seconds=MAX_CLOCK_SKEW_SECONDS + 30
    )
    with pytest.raises(ValidationError):
        TelemetryEvent(**make_payload(timestamp_utc=too_far.isoformat()))


def test_timestamp_within_clock_skew_allowance_is_accepted():
    slightly_ahead = datetime.now(timezone.utc) + timedelta(
        seconds=MAX_CLOCK_SKEW_SECONDS - 5
    )
    TelemetryEvent(**make_payload(timestamp_utc=slightly_ahead.isoformat()))


def test_stale_timestamp_in_the_past_is_accepted():
    # Old/late events are a failure-policy concern (they just don't move
    # current state backwards), not a validation rejection.
    old = datetime.now(timezone.utc) - timedelta(hours=6)
    TelemetryEvent(**make_payload(timestamp_utc=old.isoformat()))


def test_unknown_extra_fields_are_preserved_not_interpreted():
    payload = make_payload(firmware_version="1.2.3", signal_dbm=-67)
    event = TelemetryEvent(**payload)
    assert event.extra_fields() == {"firmware_version": "1.2.3", "signal_dbm": -67}


def test_no_extra_fields_returns_empty_dict():
    event = TelemetryEvent(**make_payload())
    assert event.extra_fields() == {}
