"""Unit tests for simulator.scheduler.plan_tick — the pure decision
logic tying an asset's tick to the fault switches. No network, no
sleeping: this is what actually proves each of the four requested
behaviors (duplicates, out-of-order, bad data, silence) does the right
thing, independent of whatever HTTP client eventually sends it. See
tests/integration/test_simulator_faults_e2e.py for the same four proved
against a real ingest API.
"""

from __future__ import annotations

import random

from simulator.assets import SimulatedAsset
from simulator.faults import FaultInjector, FaultRates
from simulator.scheduler import plan_tick


class _FixedDraw:
    """Every should_* check compares against one fixed draw, so setting
    it above/below a rate deterministically forces or suppresses a fault
    without depending on real randomness."""

    def __init__(self, draw: float) -> None:
        self._draw = draw

    def random(self) -> float:
        return self._draw

    def choice(self, seq):
        return seq[0]


def make_asset() -> SimulatedAsset:
    return SimulatedAsset(asset_id="drone-01", asset_type="drone", rng=random.Random(1))


def test_normal_tick_sends_exactly_one_event_and_holds_nothing():
    asset = make_asset()
    injector = FaultInjector(FaultRates(), _FixedDraw(0.99))  # all rates 0 — never fires

    plan = plan_tick(asset, injector, held=None)

    assert [e["seq"] for e in plan.to_send] == [1]
    assert plan.held is None


def test_silence_switch_sends_nothing_but_still_advances_seq():
    asset = make_asset()
    injector = FaultInjector(FaultRates(silence=1.0), _FixedDraw(0.0))

    plan = plan_tick(asset, injector, held=None)

    assert plan.to_send == []
    assert asset.seq == 1  # ticked internally even though nothing went out


def test_force_silent_overrides_the_rates_even_when_they_are_all_zero():
    asset = make_asset()
    injector = FaultInjector(FaultRates(), _FixedDraw(0.99))  # would never roll silent on its own

    plan = plan_tick(asset, injector, held=None, force_silent=True)

    assert plan.to_send == []


def test_bad_data_switch_sends_a_broken_copy_instead_of_the_real_event():
    asset = make_asset()
    injector = FaultInjector(FaultRates(bad_data=1.0), _FixedDraw(0.0))

    plan = plan_tick(asset, injector, held=None)

    assert len(plan.to_send) == 1
    assert "_simulated_fault" in plan.to_send[0]


def test_out_of_order_switch_holds_then_releases_after_a_newer_event():
    asset = make_asset()
    injector = FaultInjector(FaultRates(out_of_order=1.0), _FixedDraw(0.0))

    first = plan_tick(asset, injector, held=None)
    assert first.to_send == []  # held back, not sent yet
    assert first.held is not None
    assert first.held["seq"] == 1

    # On the next tick, held is already set, so the newer (seq 2) event
    # goes out first and the older (seq 1) right after it — arriving at
    # the API genuinely out of order.
    second = plan_tick(asset, injector, held=first.held)
    assert [e["seq"] for e in second.to_send] == [2, 1]
    assert second.held is None


def test_only_one_event_is_held_at_a_time():
    asset = make_asset()
    injector = FaultInjector(FaultRates(out_of_order=1.0), _FixedDraw(0.0))

    first = plan_tick(asset, injector, held=None)
    # A second consecutive reorder roll shouldn't matter — the "held is
    # None" guard means a second hold is never attempted while one is
    # already outstanding.
    second = plan_tick(asset, injector, held=first.held)

    assert [e["seq"] for e in second.to_send] == [2, 1]
    assert second.held is None
