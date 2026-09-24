"""GET/POST .../deadletter — inspect and replay poison messages.

The worker acks a message it can never parse off telemetry.raw
immediately (retrying it forever would wedge every message behind it),
but instead of discarding it, it preserves the exact original bytes on
telemetry.deadletter first (worker/redis_stream.py:send_to_deadletter).
This router is the "safe way to inspect and retry" half of that: read
what's sitting there, and replay one entry back onto telemetry.raw for
the worker to process again — normal service, no special-casing needed,
since it goes through the exact same validate/store/ack path any other
message does. See docs/IMPROVEMENTS.md, poison-message handling.
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError
from telemetry_contract import DEADLETTER_STREAM_NAME, STREAM_NAME

router = APIRouter(prefix="/api/v1/deadletter", tags=["deadletter"])
logger = logging.getLogger("api.deadletter")

LIST_DEFAULT_LIMIT = 50
LIST_MAX_LIMIT = 500


def _entry_to_dict(entry_id: str, fields: dict) -> dict:
    # payload is kept as the original raw JSON string (send_to_deadletter
    # never re-serializes it) — parsed here only for display, so a
    # reviewer can read it without also decoding it by hand. If it
    # doesn't even parse as JSON, show it as-is rather than fail the
    # whole listing over one unreadable entry.
    raw_payload = fields.get("payload", "")
    try:
        payload = json.loads(raw_payload) if raw_payload else None
    except json.JSONDecodeError:
        payload = raw_payload
    return {
        "deadletter_id": entry_id,
        "original_message_id": fields.get("original_message_id"),
        "asset_id": fields.get("original_asset_id") or None,
        "seq": fields.get("original_seq") or None,
        "reason": fields.get("reason"),
        "failed_at": fields.get("failed_at"),
        "received_at": fields.get("received_at"),
        "payload": payload,
    }


@router.get("")
async def list_deadletter(
    request: Request,
    limit: int = Query(LIST_DEFAULT_LIMIT, ge=1, le=LIST_MAX_LIMIT),
):
    """Newest-first, same convention as GET .../history."""
    redis = request.app.state.redis
    try:
        # XREVRANGE, not XRANGE: newest-first without the caller having to
        # reverse it themselves, matching the history endpoint's convention.
        raw_entries = await redis.xrevrange(DEADLETTER_STREAM_NAME, count=limit)
    except RedisError as exc:
        logger.error(
            "deadletter.list_failed",
            extra={"event": "deadletter.list_failed", "reason": str(exc)},
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error": "queue_unavailable",
                "detail": "could not reach the dead-letter stream; retry later",
            },
        )
    entries = [_entry_to_dict(entry_id, fields) for entry_id, fields in raw_entries]
    return {"count": len(entries), "entries": entries}


@router.post("/{deadletter_id}/replay")
async def replay_deadletter(deadletter_id: str, request: Request):
    """Put one dead-lettered message back onto telemetry.raw for the
    worker to process again, then remove it from the dead-letter stream
    so the list only ever shows what's still actually outstanding.

    No special replay path in the worker — this is exactly what
    api/routers/ingest.py's XADD does, just re-using the entry's original
    fields instead of a freshly-validated TelemetryEvent, since a
    dead-lettered payload may be exactly what the worker still can't
    parse (nothing here re-validates it) or may now succeed because
    whatever caused the original failure was fixed elsewhere in the
    meantime. Either way, if it fails again, it gets dead-lettered again
    — safe, since each replay is one deliberate action, not an automatic
    retry loop.
    """
    redis = request.app.state.redis
    try:
        # XRANGE with the same id as both bounds fetches exactly that one
        # entry — the documented way to look up a single Stream entry by
        # id without scanning the whole stream.
        found = await redis.xrange(
            DEADLETTER_STREAM_NAME, min=deadletter_id, max=deadletter_id
        )
    except RedisError as exc:
        logger.error(
            "deadletter.replay_failed",
            extra={
                "event": "deadletter.replay_failed",
                "deadletter_id": deadletter_id,
                "reason": str(exc),
            },
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error": "queue_unavailable",
                "detail": "could not reach the dead-letter stream; retry later",
            },
        )

    if not found:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"error": "deadletter_entry_not_found", "deadletter_id": deadletter_id},
        )

    _entry_id, fields = found[0]
    new_stream_id = await redis.xadd(
        STREAM_NAME,
        {
            "asset_id": fields.get("original_asset_id", ""),
            "seq": fields.get("original_seq", ""),
            "received_at": fields.get("received_at", ""),
            "payload": fields.get("payload", ""),
        },
    )
    await redis.xdel(DEADLETTER_STREAM_NAME, deadletter_id)

    logger.info(
        "deadletter.replayed",
        extra={
            "event": "deadletter.replayed",
            "deadletter_id": deadletter_id,
            "original_message_id": fields.get("original_message_id"),
            "new_message_id": new_stream_id,
        },
    )
    return {
        "status": "replayed",
        "deadletter_id": deadletter_id,
        "new_message_id": new_stream_id,
    }
