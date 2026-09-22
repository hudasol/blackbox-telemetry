"""Environment-driven settings for the simulator.

The simulator is test equipment, not part of the product (docs/PLAN.md,
problem framing) — its whole job is to exercise the real system, over
real HTTP, exactly like a real asset would, including the ways a real
asset misbehaves. Each fault below is a 0.0-1.0 probability, applied
independently per tick; 0.0 (the default) means "off".
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _float_env(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _csv_env(name: str) -> tuple[str, ...]:
    raw = os.environ.get(name)
    if not raw:
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


@dataclass(frozen=True)
class AssetSpec:
    asset_id: str
    asset_type: str


# A small mixed fleet covering all three documented asset types — enough
# to exercise the system without needing configuration for a first run.
DEFAULT_FLEET: tuple[AssetSpec, ...] = (
    AssetSpec("drone-01", "drone"),
    AssetSpec("rover-01", "rover"),
    AssetSpec("quad-01", "quadruped"),
)


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    tick_interval_seconds: float
    request_timeout_seconds: float
    log_level: str
    seed: int | None

    # Fault-injection switches.
    duplicate_rate: float
    out_of_order_rate: float
    bad_data_rate: float
    silence_rate: float

    # Asset ids that never send, regardless of silence_rate — a
    # deliberate, noise-free way to demo the offline-timeout path,
    # separate from the probabilistic silence_rate sprinkled across the
    # whole fleet.
    force_silent_asset_ids: tuple[str, ...]

    fleet: tuple[AssetSpec, ...]


def get_settings() -> Settings:
    seed_raw = os.environ.get("SIM_SEED")
    return Settings(
        api_base_url=os.environ.get("SIM_API_BASE_URL", "http://localhost:8000"),
        tick_interval_seconds=_float_env("SIM_TICK_INTERVAL_SECONDS", 1.0),
        request_timeout_seconds=_float_env("SIM_REQUEST_TIMEOUT_SECONDS", 5.0),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        seed=int(seed_raw) if seed_raw else None,
        duplicate_rate=_float_env("SIM_DUPLICATE_RATE", 0.0),
        out_of_order_rate=_float_env("SIM_OUT_OF_ORDER_RATE", 0.0),
        bad_data_rate=_float_env("SIM_BAD_DATA_RATE", 0.0),
        silence_rate=_float_env("SIM_SILENCE_RATE", 0.0),
        force_silent_asset_ids=_csv_env("SIM_FORCE_SILENT_ASSET_IDS"),
        fleet=DEFAULT_FLEET,
    )
