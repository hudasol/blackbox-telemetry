"""Shared constants for the telemetry contract.

Centralized here so api, worker, and simulator agree on the same values
without redefining them. See docs/PLAN.md for the reasoning.
"""

from __future__ import annotations

# An asset is offline when no accepted event has been seen for at least
# this many seconds. This is only the default — services read the real
# value from the ASSET_OFFLINE_TIMEOUT_SECONDS environment variable, so
# it's never a buried magic number (see PLAN.md, decision D3).
DEFAULT_ASSET_OFFLINE_TIMEOUT_SECONDS = 10

# How far into the future timestamp_utc may be before it's treated as a
# broken clock rather than ordinary network latency.
MAX_CLOCK_SKEW_SECONDS = 60

BATTERY_PCT_MIN = 0.0
BATTERY_PCT_MAX = 100.0

LINK_QUALITY_PCT_MIN = 0.0
LINK_QUALITY_PCT_MAX = 100.0

HEADING_DEG_MIN = 0.0
HEADING_DEG_MAX = 360.0

MIN_SEQ = 0

# Redis Streams naming. api (producer) and worker (consumer) must agree on
# these exactly, so they live here instead of being redefined in each
# service (see docs/PLAN.md, architecture / decision D5).
STREAM_NAME = "telemetry.raw"
CONSUMER_GROUP_NAME = "worker-group"

# A separate stream, not a consumer group on telemetry.raw, and not a
# Postgres table: worker (writer, on a poison message) and api (reader,
# for inspect/replay) both only need plain Redis Streams commands
# (XADD/XRANGE/XDEL) they already use elsewhere, with no new dependency
# and no schema change. See docs/IMPROVEMENTS.md, poison-message handling.
DEADLETTER_STREAM_NAME = "telemetry.deadletter"
