"""A single simulated asset's evolving physical state.

Deliberately simple, non-realistic physics — the point is a plausible,
continuously-varying telemetry stream that stays within the shared
contract's bounds, not an accurate robot simulator. See
tests/unit/test_simulator_assets.py, which checks exactly that: every
tick's output is bound-safe and passes TelemetryEvent validation as-is.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class SimulatedAsset:
    asset_id: str
    asset_type: str
    rng: random.Random
    seq: int = 0
    x_m: float = 0.0
    y_m: float = 0.0
    z_m: float = 0.0
    speed_mps: float = 0.0
    heading_deg: float = 0.0
    battery_pct: float = 100.0
    temperature_c: float = 25.0
    link_quality_pct: float = 95.0
    mode: str = "idle"

    def _advance_mode(self) -> None:
        # Battery takes priority over whatever the random walk below
        # would otherwise pick — a flat battery always means "charging",
        # a full one always means "back to idle".
        if self.battery_pct <= 10 and self.mode != "charging":
            self.mode = "charging"
        elif self.mode == "charging" and self.battery_pct >= 95:
            self.mode = "idle"
        elif self.mode == "idle" and self.rng.random() < 0.3:
            self.mode = "moving"
        elif self.mode == "moving" and self.rng.random() < 0.2:
            self.mode = "returning"
        elif self.mode == "returning" and self.rng.random() < 0.4:
            self.mode = "idle"

    def _advance_physics(self) -> None:
        moving = self.mode in ("moving", "returning")
        self.speed_mps = round(self.rng.uniform(0.5, 3.5), 2) if moving else 0.0

        if moving:
            self.heading_deg = (self.heading_deg + self.rng.uniform(-20, 20)) % 360
            self.x_m += self.speed_mps * self.rng.uniform(0.5, 1.0)
            self.y_m += self.speed_mps * self.rng.uniform(-0.5, 0.5)
            self.z_m = max(0.0, self.z_m + self.rng.uniform(-0.2, 0.2))

        if self.mode == "charging":
            self.battery_pct = min(100.0, self.battery_pct + self.rng.uniform(1.0, 3.0))
        elif moving:
            self.battery_pct = max(0.0, self.battery_pct - self.rng.uniform(0.3, 0.8))
        else:
            self.battery_pct = max(0.0, self.battery_pct - self.rng.uniform(0.02, 0.08))

        self.temperature_c = max(
            -10.0, min(60.0, self.temperature_c + self.rng.uniform(-0.5, 0.5))
        )
        self.link_quality_pct = max(
            0.0, min(100.0, self.link_quality_pct + self.rng.uniform(-3.0, 3.0))
        )

    def tick(self) -> dict:
        """Advance one step of state and return the wire-shaped event.

        seq always increments here, even if the caller (see
        simulator/scheduler.py) decides not to actually send this tick —
        a silent asset's onboard clock keeps ticking, it just doesn't
        transmit, which is why a real gap in seq is a harmless, expected
        thing rather than a sign of loss (docs/PLAN.md, failure policy).
        """
        self.seq += 1
        self._advance_mode()
        self._advance_physics()
        return {
            "asset_id": self.asset_id,
            "asset_type": self.asset_type,
            "seq": self.seq,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "position": {
                "x_m": round(self.x_m, 3),
                "y_m": round(self.y_m, 3),
                "z_m": round(self.z_m, 3),
            },
            "speed_mps": self.speed_mps,
            "heading_deg": round(self.heading_deg, 2),
            "battery_pct": round(self.battery_pct, 2),
            "temperature_c": round(self.temperature_c, 2),
            "link_quality_pct": round(self.link_quality_pct, 2),
            "mode": self.mode,
        }
