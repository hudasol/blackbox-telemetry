"""GET /api/v1/assets, .../latest, .../history — the read side of the
system (docs/PLAN.md, problem framing: "a query API (assets, latest,
bounded history)"). Read-only: this service never writes to Postgres,
only the worker does.
"""

from __future__ import annotations

import logging

from asyncpg import PostgresError
from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse

from .. import db

router = APIRouter(prefix="/api/v1", tags=["query"])
logger = logging.getLogger("api.query")

HISTORY_DEFAULT_LIMIT = 50
HISTORY_MAX_LIMIT = 500


def _query_unavailable() -> JSONResponse:
    # Mirrors ingest's "Redis unavailable" -> 503 (docs/PLAN.md, failure
    # policy) — this service's other core dependency, unreachable, gets
    # the same treatment: say so plainly, never fake an empty result.
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "error": "query_unavailable",
            "detail": "could not reach the database; retry later",
        },
    )


@router.get("/assets")
async def list_assets(request: Request):
    pool = request.app.state.pg_pool
    settings = request.app.state.settings
    try:
        assets = await db.list_assets(pool, settings.asset_offline_timeout_seconds)
    except (PostgresError, OSError) as exc:
        logger.error(
            "query.assets_failed",
            extra={"event": "query.assets_failed", "reason": str(exc)},
        )
        return _query_unavailable()
    return {"assets": assets}


@router.get("/assets/{asset_id}/latest")
async def get_asset_latest(asset_id: str, request: Request):
    pool = request.app.state.pg_pool
    settings = request.app.state.settings
    try:
        asset = await db.get_asset_latest(
            pool, asset_id, settings.asset_offline_timeout_seconds
        )
    except (PostgresError, OSError) as exc:
        logger.error(
            "query.latest_failed",
            extra={
                "event": "query.latest_failed",
                "asset_id": asset_id,
                "reason": str(exc),
            },
        )
        return _query_unavailable()
    if asset is None:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"error": "asset_not_found", "asset_id": asset_id},
        )
    return asset


@router.get("/assets/{asset_id}/history")
async def get_asset_history(
    asset_id: str,
    request: Request,
    limit: int = Query(HISTORY_DEFAULT_LIMIT, ge=1, le=HISTORY_MAX_LIMIT),
):
    pool = request.app.state.pg_pool
    try:
        events = await db.get_asset_history(pool, asset_id, limit)
    except (PostgresError, OSError) as exc:
        logger.error(
            "query.history_failed",
            extra={
                "event": "query.history_failed",
                "asset_id": asset_id,
                "reason": str(exc),
            },
        )
        return _query_unavailable()
    # An asset with no history yet (unknown, or simply hasn't sent
    # anything) is a plain empty result, not a 404 — unlike /latest,
    # "no history" is a perfectly valid answer, not a missing resource.
    return {"asset_id": asset_id, "limit": limit, "count": len(events), "events": events}
