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
import re

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError
from telemetry_contract import DEADLETTER_STREAM_NAME, STREAM_NAME

from ..metrics import INGEST_EVENTS_TOTAL

router = APIRouter(prefix="/api/v1/deadletter", tags=["deadletter"])
logger = logging.getLogger("api.deadletter")

LIST_DEFAULT_LIMIT = 50
LIST_MAX_LIMIT = 500

# Every real dead-letter id is a Redis Stream id in this exact shape —
# it's what XADD generates and what list_deadletter above hands back
# verbatim as "deadletter_id". Anything else (a typo, a stray path
# segment, an id copied from somewhere else) can never be a real entry,
# and XRANGE rejects it outright with "ERR Invalid stream ID..." — a
# ResponseError, which is a RedisError subclass, so it used to fall into
# the same except block as an actual Redis outage and come back as a
# misleading 503. Checked here, before Redis is ever touched, so a
# malformed id is reported as what it is instead.
_DEADLETTER_ID_PATTERN = re.compile(r"^[0-9]+-[0-9]+$")

# Replay used to be two separate round-trips — XADD onto telemetry.raw,
# then XDEL off telemetry.deadletter — with nothing tying them together.
# A Redis blip between them left a message live on telemetry.raw while
# still listed as dead-lettered, so a retried replay request re-added it
# a second time; two concurrent replays of the same id raced the same
# way with no blip needed at all, since XRANGE doesn't remove what it
# reads. A Lua script makes "is it still there, move it, remove it" one
# atomic operation on the server: it either runs to completion (moved
# and removed) or doesn't run at all (untouched) — no window where only
# half of it happened, and a second caller's script sees the entry
# already gone and cleanly no-ops instead of re-enqueuing it.
_REPLAY_SCRIPT = """
local entries = redis.call('XRANGE', KEYS[2], ARGV[1], ARGV[1])
if #entries == 0 then
    return false
end
local raw = entries[1][2]
local fields = {}
for i = 1, #raw, 2 do
    fields[raw[i]] = raw[i + 1]
end
local new_id = redis.call('XADD', KEYS[1], '*',
    'asset_id', fields['original_asset_id'] or '',
    'seq', fields['original_seq'] or '',
    'received_at', fields['received_at'] or '',
    'payload', fields['payload'] or '')
redis.call('XDEL', KEYS[2], ARGV[1])
return {new_id, fields['original_message_id'] or ''}
"""


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

    The lookup, the re-enqueue, and the removal all happen inside one
    Lua script (_REPLAY_SCRIPT) so they're atomic on the server — see
    that script's own comment for why.
    """
    if not _DEADLETTER_ID_PATTERN.match(deadletter_id):
        logger.warning(
            "deadletter.replay_invalid_id",
            extra={
                "event": "deadletter.replay_invalid_id",
                "deadletter_id": deadletter_id,
            },
        )
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "error": "invalid_deadletter_id",
                "detail": (
                    f"{deadletter_id!r} is not a valid dead-letter id "
                    "(expected <ms>-<seq>, e.g. 1690000000000-0)"
                ),
                "deadletter_id": deadletter_id,
            },
        )

    redis = request.app.state.redis
    try:
        result = await redis.eval(
            _REPLAY_SCRIPT, 2, STREAM_NAME, DEADLETTER_STREAM_NAME, deadletter_id
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

    if not result:
        # Either it never existed, or it was already replayed — by this
        # same request retried, or by a concurrent one — and the script
        # correctly no-op'd rather than enqueueing it a second time.
        # Both look identical from the caller's side: there is nothing
        # left at this id to replay.
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"error": "deadletter_entry_not_found", "deadletter_id": deadletter_id},
        )

    new_stream_id, original_message_id = result
    # The script's XADD above just put a real message onto telemetry.raw
    # — the same queue POST /api/v1/telemetry writes to — so it's real
    # traffic reaching the pipeline, not something separate from what
    # blackbox_ingest_events_total is meant to answer for. See that
    # metric's own comment (app/metrics.py) for why this is its own
    # "replayed" outcome rather than counted as "accepted".
    INGEST_EVENTS_TOTAL.labels(outcome="replayed").inc()
    logger.info(
        "deadletter.replayed",
        extra={
            "event": "deadletter.replayed",
            "deadletter_id": deadletter_id,
            "original_message_id": original_message_id or None,
            "new_message_id": new_stream_id,
        },
    )
    return {
        "status": "replayed",
        "deadletter_id": deadletter_id,
        "new_message_id": new_stream_id,
    }
