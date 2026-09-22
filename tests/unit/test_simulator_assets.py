"""Unit tests for simulator.assets.SimulatedAsset — the fake fleet's
per-tick physics. Only checks it stays within the shared contract's
bounds and that seq is monotonic; the exact motion is deliberately not
realistic (see the module's docstring).
"""

from __future__ import annotations

import random

from telemetry_contract import TelemetryEvent

from simulator.assets import SimulatedAsset


def test_seq_increments_by_one_every_tick():
    asset = SimulatedAsset(asset_id="drone-01", asset_type="drone", rng=random.Random(1))
    seqs = [asset.tick()["seq"] for _ in range(20)]
    assert seqs == list(range(1, 21))


def test_every_tick_satisfies_the_shared_contract():
    asset = SimulatedAsset(asset_id="rover-01", asset_type="rover", rng=random.Random(2))
    for _ in range(500):
        event = asset.tick()
        TelemetryEvent(**event)  # raises if any field ever drifts out of contract bounds


def test_battery_link_and_heading_stay_in_bounds_even_from_the_edges():
    asset = SimulatedAsset(
        asset_id="quad-01",
        asset_type="quadruped",
        rng=random.Random(3),
        battery_pct=1.0,
        link_quality_pct=1.0,
    )
    for _ in range(500):
        event = asset.tick()
        assert 0.0 <= event["battery_pct"] <= 100.0
        assert 0.0 <= event["link_quality_pct"] <= 100.0
        assert 0.0 <= event["heading_deg"] < 360.0


def test_low_battery_eventually_forces_charging_mode():
    asset = SimulatedAsset(
        asset_id="drone-01", asset_type="drone", rng=random.Random(4), battery_pct=9.0
    )
    modes = {asset.tick()["mode"] for _ in range(20)}
    assert "charging" in modes


def test_position_only_changes_while_moving_or_returning():
    asset = SimulatedAsset(
        asset_id="drone-01", asset_type="drone", rng=random.Random(5), mode="idle"
    )
    # Force idle forever by never letting the random-walk mode transition
    # win: with a fixed rng seed this is flaky in principle, so instead
    # just check the invariant directly — whenever mode stays idle, x/y/z
    # don't move.
    last_position = (asset.x_m, asset.y_m, asset.z_m)
    for _ in range(50):
        event = asset.tick()
        if event["mode"] == "idle":
            assert (asset.x_m, asset.y_m, asset.z_m) == last_position
        last_position = (asset.x_m, asset.y_m, asset.z_m)
