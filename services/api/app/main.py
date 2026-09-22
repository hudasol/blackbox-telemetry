"""FastAPI app entrypoint.

Ingest and query share one service (docs/PLAN.md, decision D4): ingest
depends only on Redis, query only on Postgres, health checks both.

create_app() is a factory (not a single module-level instance) so tests
can build an app pointed at different dependencies — including a
deliberately unreachable Redis or Postgres, to exercise the "dependency
unavailable" failure paths.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from urllib.parse import urlsplit, urlunsplit

import asyncpg
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from .config import Settings, get_settings
from .logging_config import configure_logging
from .routers import health, ingest, query

logger = logging.getLogger("api")


def _redact_dsn(dsn: str) -> str:
    """Strip credentials before a connection string ever reaches a log
    line — both settings.redis_url and settings.postgres_dsn can carry a
    plaintext password."""
    parts = urlsplit(dsn)
    if parts.username or parts.password:
        netloc = parts.hostname or ""
        if parts.port:
            netloc += f":{parts.port}"
        parts = parts._replace(netloc=netloc)
    return urlunsplit(parts)


async def _validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Reject with a useful error, and log it the same way an accepted
    event is logged, so a rejection is just as traceable (docs/PLAN.md,
    problem framing).

    This handler is global, so it also catches query-param validation
    errors (e.g. GET .../history?limit=0), not just a bad ingest body —
    the error code and log event name reflect which kind actually
    failed, rather than assuming every 422 is a rejected telemetry
    event.
    """
    body = exc.body if isinstance(exc.body, dict) else {}
    # exc.errors() can carry a raw exception object under ctx.error (e.g.
    # from a @model_validator ValueError) — not JSON-serializable, and
    # `msg` already has the human-readable text, so drop ctx rather than
    # let a malformed request 500 instead of 422. jsonable_encoder is a
    # safety net for anything else non-serializable in `input`.
    errors = jsonable_encoder(
        [{k: v for k, v in err.items() if k != "ctx"} for err in exc.errors()]
    )
    is_body_error = any(err.get("loc", (None,))[0] == "body" for err in exc.errors())
    error_code, log_event = (
        ("invalid_telemetry_event", "telemetry.rejected")
        if is_body_error
        else ("invalid_request", "request.rejected")
    )

    logger.warning(
        log_event,
        extra={
            "event": log_event,
            "path": str(request.url.path),
            "asset_id": body.get("asset_id"),
            "seq": body.get("seq"),
            "errors": errors,
        },
    )
    return JSONResponse(status_code=422, content={"error": error_code, "detail": errors})


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging(settings.log_level)
        app.state.settings = settings
        app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
        # min_size=0: query's Postgres dependency is independent of
        # ingest's Redis one (docs/PLAN.md, D4) — a Postgres outage at
        # startup must not stop this process from serving ingest traffic.
        # Connections open lazily on first use instead of up front.
        app.state.pg_pool = await asyncpg.create_pool(
            settings.postgres_dsn, min_size=0, max_size=5
        )
        logger.info(
            "api.startup",
            extra={
                "event": "api.startup",
                "redis_url": _redact_dsn(settings.redis_url),
                "postgres_dsn": _redact_dsn(settings.postgres_dsn),
            },
        )
        try:
            yield
        finally:
            await app.state.redis.aclose()
            await app.state.pg_pool.close()
            logger.info("api.shutdown", extra={"event": "api.shutdown"})

    app = FastAPI(title="blackbox telemetry api", lifespan=lifespan)
    app.include_router(ingest.router)
    app.include_router(query.router)
    app.include_router(health.router)
    app.add_exception_handler(RequestValidationError, _validation_exception_handler)
    return app


app = create_app()
