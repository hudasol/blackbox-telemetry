"""POST /api/v1/telemetry — the one door into the system.

Validates against the shared telemetry contract and hands accepted events
to Redis. The one rule everything else here follows: never return 2xx for
an event that wasn't durably enqueued (docs/PLAN.md, failure policy).
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError
from telemetry_contract import STREAM_NAME, TelemetryEvent

router = APIRouter(prefix="/api/v1", tags=["ingest"])
logger = logging.getLogger("api.ingest")


@router.post(
    "/telemetry",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Accept one telemetry event",
)
async def ingest_telemetry(event: TelemetryEvent, request: Request):
    """Validate (via the TelemetryEvent type) then enqueue to Redis.

    A validation failure never reaches this function — FastAPI raises
    RequestValidationError first, handled in app.main so it can still be
    logged with the same structured fields.
    """
    redis = request.app.state.redis
    request_id = str(uuid.uuid4())
    received_at = datetime.now(timezone.utc)
    start = time.monotonic()

    try:
        stream_id = await redis.xadd(
            STREAM_NAME,
            {
                "asset_id": event.asset_id,
                "seq": str(event.seq),
                "received_at": received_at.isoformat(),
                # Canonical, fully-validated event (extra fields included,
                # never interpreted — just carried through).
                "payload": event.model_dump_json(),
            },
        )
    except RedisError as exc:
        logger.error(
            "telemetry.enqueue_failed",
            extra={
                "event": "telemetry.enqueue_failed",
                "asset_id": event.asset_id,
                "seq": event.seq,
                "request_id": request_id,
                "reason": str(exc),
            },
        )
        # Redis is down: say so plainly. Never pretend this was accepted
        # (docs/PLAN.md, failure policy — "Redis unavailable").
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error": "queue_unavailable",
                "detail": "telemetry could not be durably accepted; retry later",
            },
        )

    latency_ms = round((time.monotonic() - start) * 1000, 2)
    logger.info(
        "telemetry.enqueued",
        extra={
            "event": "telemetry.enqueued",
            "asset_id": event.asset_id,
            "seq": event.seq,
            "asset_type": event.asset_type.value,
            "request_id": request_id,
            "stream_id": stream_id,
            "latency_ms": latency_ms,
        },
    )
    return {
        "status": "accepted",
        "asset_id": event.asset_id,
        "seq": event.seq,
        "stream_id": stream_id,
    }
