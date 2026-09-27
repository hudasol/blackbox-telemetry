"""Postgres access: schema bootstrap, idempotent insert, monotonic upsert.

Two tables:
  telemetry_events — raw accepted events, append-only, the full history.
                      (asset_id, seq) is unique: the idempotency key from
                      docs/PLAN.md, decision D1/D2.
  asset_state       — one row per asset, the latest snapshot. Updated only
                      when the incoming seq is strictly newer than what's
                      already stored there — "state never regresses"
                      (docs/PLAN.md, failure policy).

Both writes happen in a single transaction, so an event can never end up
recorded in history but not reflected in current state, or vice versa.

No Alembic: two tables, `CREATE TABLE IF NOT EXISTS` run at startup is the
whole migration story for this scope, and it doubles as what makes the
service boot with no manual setup step (docs/PROCESS_LOG.md).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

import asyncpg
from telemetry_contract import TelemetryEvent

logger = logging.getLogger("worker.db")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS telemetry_events (
    id                  BIGSERIAL PRIMARY KEY,
    asset_id            TEXT NOT NULL,
    asset_type          TEXT NOT NULL,
    seq                 BIGINT NOT NULL,
    timestamp_utc       TIMESTAMPTZ NOT NULL,
    server_received_at  TIMESTAMPTZ NOT NULL,
    position_x_m        DOUBLE PRECISION NOT NULL,
    position_y_m        DOUBLE PRECISION NOT NULL,
    position_z_m        DOUBLE PRECISION NOT NULL,
    speed_mps           DOUBLE PRECISION NOT NULL,
    heading_deg         DOUBLE PRECISION NOT NULL,
    battery_pct         DOUBLE PRECISION NOT NULL,
    temperature_c       DOUBLE PRECISION NOT NULL,
    link_quality_pct    DOUBLE PRECISION NOT NULL,
    mode                TEXT NOT NULL,
    extra_fields        JSONB NOT NULL DEFAULT '{}'::jsonb,
    inserted_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_telemetry_events_asset_seq UNIQUE (asset_id, seq)
);

CREATE INDEX IF NOT EXISTS ix_telemetry_events_asset_ts
    ON telemetry_events (asset_id, timestamp_utc);

CREATE TABLE IF NOT EXISTS asset_state (
    asset_id             TEXT PRIMARY KEY,
    asset_type           TEXT NOT NULL,
    latest_seq           BIGINT NOT NULL,
    latest_timestamp_utc TIMESTAMPTZ NOT NULL,
    last_seen_at         TIMESTAMPTZ NOT NULL,
    position_x_m         DOUBLE PRECISION NOT NULL,
    position_y_m         DOUBLE PRECISION NOT NULL,
    position_z_m         DOUBLE PRECISION NOT NULL,
    speed_mps             DOUBLE PRECISION NOT NULL,
    heading_deg           DOUBLE PRECISION NOT NULL,
    battery_pct           DOUBLE PRECISION NOT NULL,
    temperature_c         DOUBLE PRECISION NOT NULL,
    link_quality_pct      DOUBLE PRECISION NOT NULL,
    mode                  TEXT NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

_INSERT_EVENT_SQL = """
INSERT INTO telemetry_events (
    asset_id, asset_type, seq, timestamp_utc, server_received_at,
    position_x_m, position_y_m, position_z_m, speed_mps, heading_deg,
    battery_pct, temperature_c, link_quality_pct, mode, extra_fields
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::jsonb)
ON CONFLICT (asset_id, seq) DO NOTHING
RETURNING id
"""

# The WHERE clause is what makes "never regress" atomic and race-safe: if
# the row already has a latest_seq >= the incoming one, the UPDATE simply
# matches zero rows and RETURNING gives back nothing — no read-then-write
# needed, no window for a concurrent writer to sneak a regression in.
_UPSERT_STATE_SQL = """
INSERT INTO asset_state (
    asset_id, asset_type, latest_seq, latest_timestamp_utc, last_seen_at,
    position_x_m, position_y_m, position_z_m, speed_mps, heading_deg,
    battery_pct, temperature_c, link_quality_pct, mode
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT (asset_id) DO UPDATE SET
    asset_type = EXCLUDED.asset_type,
    latest_seq = EXCLUDED.latest_seq,
    latest_timestamp_utc = EXCLUDED.latest_timestamp_utc,
    last_seen_at = EXCLUDED.last_seen_at,
    position_x_m = EXCLUDED.position_x_m,
    position_y_m = EXCLUDED.position_y_m,
    position_z_m = EXCLUDED.position_z_m,
    speed_mps = EXCLUDED.speed_mps,
    heading_deg = EXCLUDED.heading_deg,
    battery_pct = EXCLUDED.battery_pct,
    temperature_c = EXCLUDED.temperature_c,
    link_quality_pct = EXCLUDED.link_quality_pct,
    mode = EXCLUDED.mode,
    updated_at = now()
WHERE asset_state.latest_seq < EXCLUDED.latest_seq
RETURNING asset_id
"""


async def create_pool(dsn: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, min_size=1, max_size=5)


async def bootstrap_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)


class StoreResult:
    """What actually happened, so the caller can log it precisely instead
    of just "it worked"."""

    __slots__ = ("event_inserted", "state_advanced")

    def __init__(self, event_inserted: bool, state_advanced: bool) -> None:
        self.event_inserted = event_inserted
        self.state_advanced = state_advanced

    @property
    def outcome(self) -> str:
        if not self.event_inserted:
            return "duplicate_ignored"
        if self.state_advanced:
            return "stored_and_advanced"
        return "stored_but_stale"


async def store_event(
    pool: asyncpg.Pool, event: TelemetryEvent, server_received_at: datetime
) -> StoreResult:
    async with pool.acquire() as conn, conn.transaction():
        inserted_id = await conn.fetchval(
            _INSERT_EVENT_SQL,
            event.asset_id,
            event.asset_type.value,
            event.seq,
            event.timestamp_utc,
            server_received_at,
            event.position.x_m,
            event.position.y_m,
            event.position.z_m,
            event.speed_mps,
            event.heading_deg,
            event.battery_pct,
            event.temperature_c,
            event.link_quality_pct,
            event.mode.value,
            # allow_nan=False: fail fast with a ValueError here rather
            # than silently emitting non-standard NaN/Infinity tokens
            # that json.dumps allows by default but Postgres's strict
            # jsonb parser rejects — same failure, but raised on our own
            # terms instead of surfacing as an opaque PostgresError.
            # telemetry_contract.TelemetryEvent already rejects these
            # (and embedded NUL bytes) at validation time; this is the
            # backstop for anything that reaches here anyway.
            json.dumps(event.extra_fields(), allow_nan=False),
        )
        advanced_asset_id = await conn.fetchval(
            _UPSERT_STATE_SQL,
            event.asset_id,
            event.asset_type.value,
            event.seq,
            event.timestamp_utc,
            server_received_at,
            event.position.x_m,
            event.position.y_m,
            event.position.z_m,
            event.speed_mps,
            event.heading_deg,
            event.battery_pct,
            event.temperature_c,
            event.link_quality_pct,
            event.mode.value,
        )

    return StoreResult(
        event_inserted=inserted_id is not None,
        state_advanced=advanced_asset_id is not None,
    )
