"""GET /api/v1/stream — live asset-state updates over Server-Sent Events.

Subscribes to the Redis Pub/Sub channel the worker publishes to right
after each message is durably stored and acked
(worker/redis_stream.py:publish_state_update — see that function's
docstring for why this is a *best-effort* side channel with nothing
durable riding on it). This router only ever reads that channel; it
never touches telemetry.raw or the consumer group, so a slow or stuck
SSE client can't backpressure the worker or ingest in any way — Redis
Pub/Sub simply drops a publish when nobody is subscribed, and a
subscriber that falls behind just sees fresher messages, not a growing
queue anyone has to manage.

A client that wants the current state before subscribing (e.g. on first
load) should call GET .../assets or .../assets/{id}/latest first, same
as any other "snapshot then live updates" pattern — this endpoint is
deliberately just the live half, not a replacement for the query API.
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from redis.exceptions import RedisError
from telemetry_contract import STATE_UPDATE_CHANNEL_NAME

router = APIRouter(prefix="/api/v1", tags=["stream"])
logger = logging.getLogger("api.stream")

# How often to send an SSE comment line (": keep-alive") when nothing's
# been published — keeps intermediary proxies/load balancers from timing
# out an idle-looking HTTP connection, without it counting as a real event.
KEEPALIVE_SECONDS = 15.0


async def _event_source(request: Request, redis, asset_id: str | None):
    pubsub = redis.pubsub()
    await pubsub.subscribe(STATE_UPDATE_CHANNEL_NAME)
    try:
        while True:
            if await request.is_disconnected():
                break
            try:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=KEEPALIVE_SECONDS
                )
            except RedisError as exc:
                # Redis itself dropped — nothing to recover into mid-
                # stream (docs/PLAN.md has no "resume an SSE stream"
                # concept); tell the client plainly and end the stream
                # rather than spin silently. A client can just reconnect.
                logger.warning(
                    "stream.redis_error",
                    extra={"event": "stream.redis_error", "reason": str(exc)},
                )
                yield 'event: error\ndata: {"error": "queue_unavailable"}\n\n'
                break

            if message is None:
                yield ": keep-alive\n\n"
                continue

            data = message["data"]
            if asset_id is not None:
                try:
                    parsed = json.loads(data)
                except json.JSONDecodeError:
                    continue
                # publish_state_update (worker/redis_stream.py) only ever
                # publishes a JSON object — but this channel is a plain
                # Redis Pub/Sub topic, not something this router controls
                # end to end, so a non-object payload (a bare number,
                # string, list, or null all parse as valid JSON) must be
                # skipped like any other message that isn't this asset's,
                # not blow up the subscriber with an AttributeError from
                # calling .get() on something that isn't a dict.
                if not isinstance(parsed, dict) or parsed.get("asset_id") != asset_id:
                    continue
            yield f"data: {data}\n\n"
    finally:
        await pubsub.unsubscribe(STATE_UPDATE_CHANNEL_NAME)
        await pubsub.aclose()


@router.get("/stream")
async def stream(
    request: Request,
    asset_id: str | None = Query(
        None, description="Only forward updates for this asset_id; omit for every asset."
    ),
):
    redis = request.app.state.redis
    return StreamingResponse(
        _event_source(request, redis, asset_id),
        media_type="text/event-stream",
        headers={
            # Standard SSE hygiene: never let an intermediary cache or
            # buffer a stream that's live by definition.
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
