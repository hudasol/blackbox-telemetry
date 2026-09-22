"""Fault injection: decides, per tick, whether to duplicate, reorder,
corrupt, or silently drop an event — and how to corrupt one. Pure and
seed-driven, so each of the four switches the brief asks for is
independently unit-testable with no network or timing involved. See
docs/PLAN.md's failure policy for what each one is meant to exercise on
the receiving end.
"""

from __future__ import annotations

import copy
import random
from dataclasses import dataclass


@dataclass(frozen=True)
class FaultRates:
    duplicate: float = 0.0
    out_of_order: float = 0.0
    bad_data: float = 0.0
    silence: float = 0.0


# Every kind here is chosen to be a guaranteed contract-validation
# failure (see tests/unit/test_simulator_faults.py, which checks each one
# against the real TelemetryEvent model) — the point is to reliably drive
# a 422, not to occasionally get unlucky and pass validation by accident.
_CORRUPTIONS = (
    "missing_field",
    "battery_out_of_range",
    "heading_out_of_range",
    "bad_enum_mode",
    "bad_enum_asset_type",
    "negative_seq",
    "wrong_type_speed",
    "wrong_type_position",
)


class FaultInjector:
    """One instance per simulated asset (see simulator/main.py), each
    seeded from the run's overall --seed, so a single seed makes a whole
    run's fault sequence fully reproducible."""

    def __init__(self, rates: FaultRates, rng: random.Random) -> None:
        self._rates = rates
        self._rng = rng

    def should_duplicate(self) -> bool:
        return self._rng.random() < self._rates.duplicate

    def should_reorder(self) -> bool:
        return self._rng.random() < self._rates.out_of_order

    def should_corrupt(self) -> bool:
        return self._rng.random() < self._rates.bad_data

    def should_go_silent(self) -> bool:
        return self._rng.random() < self._rates.silence

    def corrupt(self, event: dict) -> dict:
        """Return a broken copy of event that the ingest API must reject
        with 422 and never enqueue (docs/PLAN.md, failure policy) — the
        real reading for this tick is simply lost, exactly like a sensor
        glitch on a real asset would be."""
        broken = copy.deepcopy(event)
        kind = self._rng.choice(_CORRUPTIONS)

        if kind == "missing_field":
            field = self._rng.choice(["asset_type", "position", "mode", "battery_pct"])
            broken.pop(field, None)
        elif kind == "battery_out_of_range":
            broken["battery_pct"] = self._rng.choice([-5.0, 150.0])
        elif kind == "heading_out_of_range":
            broken["heading_deg"] = self._rng.choice([-10.0, 720.0])
        elif kind == "bad_enum_mode":
            broken["mode"] = "flying_sideways"
        elif kind == "bad_enum_asset_type":
            broken["asset_type"] = "submarine"
        elif kind == "negative_seq":
            broken["seq"] = -1
        elif kind == "wrong_type_speed":
            broken["speed_mps"] = "not-a-number"
        elif kind == "wrong_type_position":
            broken["position"] = "not-an-object"

        broken["_simulated_fault"] = kind
        return broken
