"""Environment-driven settings.

Plain dataclass over os.environ rather than a settings library — the api
only has two knobs today, and a factory function makes it trivial for
tests to override them without touching the environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    redis_url: str
    log_level: str


def get_settings() -> Settings:
    return Settings(
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
    )
