"""Environment-driven settings. See services/api/app/config.py — same
pattern, kept per-service rather than shared since each has different
knobs and this is small enough that sharing isn't worth the coupling.
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    redis_url: str
    postgres_dsn: str
    log_level: str
    consumer_name: str


def get_settings() -> Settings:
    return Settings(
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        postgres_dsn=os.environ.get(
            "POSTGRES_DSN", "postgresql://blackbox:blackbox@localhost:5432/blackbox"
        ),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        # Defaults to the hostname (the container id, under Docker) so
        # each replica gets a stable-ish identity. Doesn't need to be
        # globally unique for correctness — XAUTOCLAIM recovers pending
        # work group-wide regardless of which consumer name originally
        # read it (see redis_stream.py).
        consumer_name=os.environ.get("WORKER_CONSUMER_NAME", socket.gethostname()),
    )
