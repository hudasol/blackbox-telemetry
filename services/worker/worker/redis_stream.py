"""Redis Streams consumer-group plumbing.

Kept separate from "what to do with a message" (processing.py) so the
mechanics of getting messages don't tangle with what happens to them.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from telemetry_contract import CONSUMER_GROUP_NAME, DEADLETTER_STREAM_NAME, STREAM_NAME

logger = logging.getLogger("worker.stream")


async def ensure_consumer_group(redis: Redis) -> None:
    """Create the consumer group at the start of the stream if it doesn't
    exist yet. Starting at "0" (not "$") means a worker that's the first
    thing to ever read this stream still sees everything already in it,
    rather than silently skipping whatever arrived before the group
    existed.
    """
    try:
        await redis.xgroup_create(
            STREAM_NAME, CONSUMER_GROUP_NAME, id="0", mkstream=True
        )
        logger.info(
            "worker.consumer_group_created",
            extra={"event": "worker.consumer_group_created"},
        )
    except ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            logger.info(
                "worker.consumer_group_exists",
                extra={"event": "worker.consumer_group_exists"},
            )
        else:
            raise


async def claim_stale_pending(
    redis: Redis, consumer_name: str, min_idle_ms: int, count: int = 50
) -> list[tuple[str, dict]]:
    """Reclaim messages that have been pending (read but never acked) for
    longer than min_idle_ms — from this worker's own earlier crash, or a
    different consumer that died. XAUTOCLAIM works group-wide regardless
    of which consumer name originally read the message, which is what
    makes "worker killed and restarted" actually recoverable rather than
    just quietly stuck (docs/PLAN.md, failure policy).
    """
    _next_cursor, messages, _deleted = await redis.xautoclaim(
        STREAM_NAME,
        CONSUMER_GROUP_NAME,
        consumer_name,
        min_idle_time=min_idle_ms,
        start_id="0-0",
        count=count,
    )
    return messages


async def read_new(
    redis: Redis, consumer_name: str, block_ms: int, count: int = 1
) -> list[tuple[str, dict]]:
    response = await redis.xreadgroup(
        CONSUMER_GROUP_NAME,
        consumer_name,
        {STREAM_NAME: ">"},
        count=count,
        block=block_ms,
    )
    if not response:
        return []
    _stream_name, messages = response[0]
    return messages


async def ack(redis: Redis, message_id: str) -> None:
    await redis.xack(STREAM_NAME, CONSUMER_GROUP_NAME, message_id)


async def send_to_deadletter(
    redis: Redis, original_message_id: str, fields: dict, reason: str
) -> str:
    """Preserve a message this worker can never successfully process,
    instead of discarding it on ack (docs/PLAN.md's old "known
    limitation" — see docs/IMPROVEMENTS.md, poison-message handling).

    A plain stream, not a consumer group: nothing auto-reads this one, an
    operator decides when to act on it. The original telemetry.raw
    message_id and fields are kept as-is (not re-parsed or reshaped) so
    replay_to_raw below can put exactly the same bytes back, and so a
    human inspecting this can see precisely what the worker saw.
    """
    entry_id = await redis.xadd(
        DEADLETTER_STREAM_NAME,
        {
            "original_message_id": original_message_id,
            "original_asset_id": fields.get("asset_id", ""),
            "original_seq": fields.get("seq", ""),
            "payload": fields.get("payload", ""),
            "received_at": fields.get("received_at", ""),
            "reason": reason,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    logger.error(
        "telemetry.deadlettered",
        extra={
            "event": "telemetry.deadlettered",
            "original_message_id": original_message_id,
            "deadletter_id": entry_id,
            "reason": reason,
        },
    )
    return entry_id
