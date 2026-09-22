"""Unit tests for simulator.faults — the pure, seedable fault-injection
logic. No network needed; corruption correctness is checked directly
against the shared contract (TelemetryEvent), the same validation the
real ingest API applies.
"""

from __future__ import annotations

import random

import pytest
from pydantic import ValidationError
from telemetry_contract import TelemetryEvent

from simulator.faults import _CORRUPTIONS, FaultInjector, FaultRates

SAMPLES = 5000
TOLERANCE = 0.04


class _FixedChoice:
    """A stub rng: `random()` always returns a fixed draw, and `choice()`
    returns a fixed value whenever it's a legal option for that call —
    letting a test force which corruption *kind* gets picked without
    also having to predict corrupt()'s second, nested choice() call (e.g.
    which field to drop for "missing_field"), which falls back to real
    randomness instead."""

    def __init__(self, draw: float, choice_value: object = None) -> None:
        self._draw = draw
        self._choice_value = choice_value
        self._fallback = random.Random(0)

    def random(self) -> float:
        return self._draw

    def choice(self, seq):
        if self._choice_value is not None and self._choice_value in seq:
            return self._choice_value
        return self._fallback.choice(seq)


def valid_event() -> dict:
    return {
        "asset_id": "drone-01",
        "asset_type": "drone",
        "seq": 1,
        "timestamp_utc": "2026-01-01T00:00:00+00:00",
        "position": {"x_m": 1.0, "y_m": 2.0, "z_m": 3.0},
        "speed_mps": 1.5,
        "heading_deg": 90.0,
        "battery_pct": 80.0,
        "temperature_c": 25.0,
        "link_quality_pct": 90.0,
        "mode": "moving",
    }


def _observed_rate(predicate) -> float:
    hits = sum(1 for _ in range(SAMPLES) if predicate())
    return hits / SAMPLES


def test_zero_rate_never_triggers():
    injector = FaultInjector(FaultRates(), random.Random(1))
    assert not any(injector.should_duplicate() for _ in range(SAMPLES))
    assert not any(injector.should_reorder() for _ in range(SAMPLES))
    assert not any(injector.should_corrupt() for _ in range(SAMPLES))
    assert not any(injector.should_go_silent() for _ in range(SAMPLES))


def test_rate_one_always_triggers():
    injector = FaultInjector(
        FaultRates(duplicate=1.0, out_of_order=1.0, bad_data=1.0, silence=1.0),
        random.Random(2),
    )
    assert all(injector.should_duplicate() for _ in range(100))
    assert all(injector.should_reorder() for _ in range(100))
    assert all(injector.should_corrupt() for _ in range(100))
    assert all(injector.should_go_silent() for _ in range(100))


@pytest.mark.parametrize(
    "kwargs,attr",
    [
        ({"duplicate": 0.1}, "should_duplicate"),
        ({"out_of_order": 0.3}, "should_reorder"),
        ({"bad_data": 0.5}, "should_corrupt"),
        ({"silence": 0.2}, "should_go_silent"),
    ],
)
def test_observed_rate_is_close_to_the_configured_rate(kwargs, attr):
    rate = next(iter(kwargs.values()))
    injector = FaultInjector(FaultRates(**kwargs), random.Random(42))
    observed = _observed_rate(getattr(injector, attr))
    assert abs(observed - rate) < TOLERANCE


@pytest.mark.parametrize("kind", _CORRUPTIONS)
def test_every_corruption_kind_fails_contract_validation(kind):
    injector = FaultInjector(FaultRates(bad_data=1.0), _FixedChoice(0.0, choice_value=kind))
    broken = injector.corrupt(valid_event())

    with pytest.raises(ValidationError):
        TelemetryEvent(**broken)


def test_corrupt_marks_which_fault_was_applied():
    injector = FaultInjector(FaultRates(bad_data=1.0), _FixedChoice(0.0))
    broken = injector.corrupt(valid_event())
    assert broken["_simulated_fault"] in _CORRUPTIONS


def test_corrupt_does_not_mutate_the_original_event():
    original = valid_event()
    injector = FaultInjector(FaultRates(bad_data=1.0), _FixedChoice(0.0, choice_value="negative_seq"))
    injector.corrupt(original)
    assert original["seq"] == 1  # untouched — corrupt() works on a copy
