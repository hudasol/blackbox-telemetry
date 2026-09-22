"""FastAPI app entrypoint.

Ingest and query share one service (docs/PLAN.md, decision D4). This file
wires up ingest now; the query router is added in a later step.

create_app() is a factory (not a single module-level instance) so tests
can build an app pointed at a different Redis — including a deliberately
unreachable one, to exercise the "Redis unavailable" failure path.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from .config import Settings, get_settings
from .logging_config import configure_logging
from .routers import ingest

logger = logging.getLogger("api")


async def _validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Reject with a useful error, and log it the same way an accepted
    event is logged, so a rejection is just as traceable (docs/PLAN.md,
    problem framing)."""
    body = exc.body if isinstance(exc.body, dict) else {}
    # exc.errors() can carry a raw exception object under ctx.error (e.g.
    # from a @model_validator ValueError) — not JSON-serializable, and
    # `msg` already has the human-readable text, so drop ctx rather than
    # let a malformed request 500 instead of 422. jsonable_encoder is a
    # safety net for anything else non-serializable in `input`.
    errors = jsonable_encoder(
        [{k: v for k, v in err.items() if k != "ctx"} for err in exc.errors()]
    )
    logger.warning(
        "telemetry.rejected",
        extra={
            "event": "telemetry.rejected",
            "asset_id": body.get("asset_id"),
            "seq": body.get("seq"),
            "path": str(request.url.path),
            "errors": errors,
        },
    )
    return JSONResponse(
        status_code=422,
        content={"error": "invalid_telemetry_event", "detail": errors},
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging(settings.log_level)
        app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
        logger.info(
            "api.startup",
            extra={"event": "api.startup", "redis_url": settings.redis_url},
        )
        try:
            yield
        finally:
            await app.state.redis.aclose()
            logger.info("api.shutdown", extra={"event": "api.shutdown"})

    app = FastAPI(title="blackbox telemetry api", lifespan=lifespan)
    app.include_router(ingest.router)
    app.add_exception_handler(RequestValidationError, _validation_exception_handler)
    return app


app = create_app()
