"""Read-side Postgres access for the query API.

The read-only counterpart to worker/db.py's schema and write path — this
service never writes to Postgres, only the worker does (docs/PLAN.md,
architecture). "online" is computed here, at read time, from
asset_state.last_seen_at — never stored as a column (decision D3).
"""

from __future__ import annotations

import json

import asyncpg

_ASSET_COLUMNS_SQL = """
    asset_id, asset_type, latest_seq, latest_timestamp_utc, last_seen_at,
    position_x_m, position_y_m, position_z_m, speed_mps, heading_deg,
    battery_pct, temperature_c, link_quality_pct, mode,
    (EXTRACT(EPOCH FROM (now() - last_seen_at)) < $1) AS online
"""

_LIST_ASSETS_SQL = f"SELECT {_ASSET_COLUMNS_SQL} FROM asset_state ORDER BY asset_id"

_GET_ASSET_SQL = f"SELECT {_ASSET_COLUMNS_SQL} FROM asset_state WHERE asset_id = $2"

_HISTORY_SQL = """
SELECT seq, timestamp_utc, server_received_at,
       position_x_m, position_y_m, position_z_m, speed_mps, heading_deg,
       battery_pct, temperature_c, link_quality_pct, mode, extra_fields
FROM telemetry_events
WHERE asset_id = $1
ORDER BY seq DESC
LIMIT $2
"""


def _asset_row_to_dict(row: asyncpg.Record) -> dict:
    return {
        "asset_id": row["asset_id"],
        "asset_type": row["asset_type"],
        "online": row["online"],
        "latest_seq": row["latest_seq"],
        "latest_timestamp_utc": row["latest_timestamp_utc"],
        "last_seen_at": row["last_seen_at"],
        "position": {
            "x_m": row["position_x_m"],
            "y_m": row["position_y_m"],
            "z_m": row["position_z_m"],
        },
        "speed_mps": row["speed_mps"],
        "heading_deg": row["heading_deg"],
        "battery_pct": row["battery_pct"],
        "temperature_c": row["temperature_c"],
        "link_quality_pct": row["link_quality_pct"],
        "mode": row["mode"],
    }


def _event_row_to_dict(row: asyncpg.Record) -> dict:
    # extra_fields comes back as raw JSON text — no type codec is
    # registered on the pool, mirroring worker/db.py's explicit
    # json.dumps on the write side rather than relying on asyncpg magic.
    extra = json.loads(row["extra_fields"]) if row["extra_fields"] else {}
    return {
        "seq": row["seq"],
        "timestamp_utc": row["timestamp_utc"],
        "server_received_at": row["server_received_at"],
        "position": {
            "x_m": row["position_x_m"],
            "y_m": row["position_y_m"],
            "z_m": row["position_z_m"],
        },
        "speed_mps": row["speed_mps"],
        "heading_deg": row["heading_deg"],
        "battery_pct": row["battery_pct"],
        "temperature_c": row["temperature_c"],
        "link_quality_pct": row["link_quality_pct"],
        "mode": row["mode"],
        "extra_fields": extra,
    }


async def list_assets(pool: asyncpg.Pool, offline_timeout_seconds: float) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(_LIST_ASSETS_SQL, offline_timeout_seconds)
    return [_asset_row_to_dict(row) for row in rows]


async def get_asset_latest(
    pool: asyncpg.Pool, asset_id: str, offline_timeout_seconds: float
) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(_GET_ASSET_SQL, offline_timeout_seconds, asset_id)
    return _asset_row_to_dict(row) if row is not None else None


async def get_asset_history(pool: asyncpg.Pool, asset_id: str, limit: int) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(_HISTORY_SQL, asset_id, limit)
    return [_event_row_to_dict(row) for row in rows]


async def ping(pool: asyncpg.Pool) -> None:
    """Raises if Postgres isn't reachable right now — used by /health/ready."""
    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
