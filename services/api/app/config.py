"""Environment-driven settings.

Plain dataclass over os.environ rather than a settings library — the api
only has a few knobs, and a factory function makes it trivial for tests
to override them without touching the environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from telemetry_contract import DEFAULT_ASSET_OFFLINE_TIMEOUT_SECONDS


@dataclass(frozen=True)
class Settings:
    # Defaults mirror get_settings()'s env-var defaults below, so tests
    # (see tests/integration/test_ingest_api.py) can construct a partial
    # Settings — e.g. just redis_url — without having to spell out every
    # field just to override one.
    redis_url: str = "redis://localhost:6379/0"
    postgres_dsn: str = "postgresql://blackbox:blackbox@localhost:5432/blackbox"
    log_level: str = "INFO"
    # How long since asset_state.last_seen_at before an asset reads as
    # offline — computed at read time, never stored (docs/PLAN.md, D3).
    asset_offline_timeout_seconds: float = float(DEFAULT_ASSET_OFFLINE_TIMEOUT_SECONDS)


def get_settings() -> Settings:
    return Settings(
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        postgres_dsn=os.environ.get(
            "POSTGRES_DSN", "postgresql://blackbox:blackbox@localhost:5432/blackbox"
        ),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        asset_offline_timeout_seconds=float(
            os.environ.get(
                "ASSET_OFFLINE_TIMEOUT_SECONDS", DEFAULT_ASSET_OFFLINE_TIMEOUT_SECONDS
            )
        ),
    )
