"""Redis Streams consumer-group plumbing.

Kept separate from "what to do with a message" (processing.py) so the
mechanics of getting messages don't tangle with what happens to them.
"""

from __future__ import annotations

import logging

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from telemetry_contract import CONSUMER_GROUP_NAME, STREAM_NAME

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
