from __future__ import annotations

import logging

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from telemetry_contract import CONSUMER_GROUP_NAME, STREAM_NAME

logger = logging.getLogger("worker.stream")


async def ensure_consumer_group(redis: Redis) -> None:
    try:
        # id="0" (not "$") so a worker starting for the first time against
        # a stream that already has entries doesn't skip them.
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
    """Reclaim entries idle longer than min_idle_ms, regardless of which
    consumer originally read them — that's what makes a worker restart
    (under any consumer name) recover a previous instance's in-flight
    work."""
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
