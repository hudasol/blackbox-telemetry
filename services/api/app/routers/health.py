"""Liveness vs. readiness (docs/PLAN.md, problem framing).

/health/live: is the process itself up and able to respond at all — no
dependency checks, so an orchestrator never restarts a perfectly healthy
process just because Redis or Postgres is having a bad moment.

/health/ready: are this service's actual dependencies reachable right
now — used to decide whether to route traffic here. Both ingest's Redis
and query's Postgres live in this one service (D4), so both are checked.
"""

from __future__ import annotations

import logging

from asyncpg import PostgresError
from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

from .. import db

router = APIRouter(tags=["health"])
logger = logging.getLogger("api.health")


@router.get("/health/live")
async def live() -> dict:
    return {"status": "alive"}


@router.get("/health/ready")
async def ready(request: Request) -> JSONResponse:
    redis = request.app.state.redis
    pool = request.app.state.pg_pool

    checks: dict[str, str] = {}
    healthy = True

    try:
        await redis.ping()
        checks["redis"] = "ok"
    except RedisError as exc:
        checks["redis"] = f"error: {exc}"
        healthy = False

    try:
        await db.ping(pool)
        checks["postgres"] = "ok"
    except (PostgresError, OSError) as exc:
        checks["postgres"] = f"error: {exc}"
        healthy = False

    if not healthy:
        logger.warning(
            "api.not_ready", extra={"event": "api.not_ready", "checks": checks}
        )

    return JSONResponse(
        status_code=status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"status": "ready" if healthy else "not_ready", "checks": checks},
    )
