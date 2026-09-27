"""Redis Streams consumer-group plumbing.

Kept separate from "what to do with a message" (processing.py) so the
mechanics of getting messages don't tangle with what happens to them.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError
from telemetry_contract import (
    CONSUMER_GROUP_NAME,
    DEADLETTER_STREAM_NAME,
    STATE_UPDATE_CHANNEL_NAME,
    STREAM_NAME,
)

logger = logging.getLogger("worker.stream")

# A per-message advisory lock (worker/processing.py) so a second worker
# replica can never run db.store_event for a message that another
# replica is still actively retrying — see acquire_message_lock's own
# docstring for the failure mode this closes.
MESSAGE_LOCK_PREFIX = "worker:lock:"


def _message_lock_key(message_id: str) -> str:
    return f"{MESSAGE_LOCK_PREFIX}{message_id}"


# Compare-and-extend / compare-and-delete: both only act if the calling
# attempt still holds the lock (its token still matches what's stored),
# so a worker whose lock already expired and was picked up by someone
# else can never stomp on that new owner's lock by extending or
# releasing it out from under them. Single EVAL is atomic on the Redis
# server, so there's no separate GET-then-EXPIRE/DEL race either.
_EXTEND_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

_RELEASE_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


async def acquire_message_lock(
    redis: Redis, message_id: str, token: str, ttl_seconds: int
) -> bool:
    """True if this attempt now owns the lock for message_id, False if
    another attempt (this replica or another) already holds it.

    Redis Streams' own idle-time bookkeeping (claim_stale_pending,
    XAUTOCLAIM) only knows "how long since this message was last
    delivered or claimed" — it has no idea whether the consumer that's
    holding it is alive and still working, just slow. With more than
    one worker replica (worker/main.py's own docstring anticipates
    --scale worker=N), a message that's mid-retry with backoff (worker
    processing.py's own store-retry loop, up to BACKOFF_MAX_SECONDS
    between attempts, with no bound on how many attempts) can sit idle
    from Redis's point of view for longer than CLAIM_MIN_IDLE_MS even
    though the original worker is still alive and will eventually
    succeed — at which point a second replica's claim_stale_pending
    legitimately reclaims it (Redis itself has no way to know better)
    and starts processing the same message a second time, concurrently.

    This lock is what actually decides "is anyone already working on
    this" independently of Streams' idle-time heuristic: the first
    attempt to acquire it wins and keeps renewing it (see
    extend_message_lock) for as long as it's actively retrying: a
    second attempt — whether it's a genuinely new delivery or a
    XAUTOCLAIM reclaim racing a still-live retry — simply fails to
    acquire it and backs off without touching the database or acking,
    leaving the message for the original owner to finish.
    """
    return bool(
        await redis.set(_message_lock_key(message_id), token, nx=True, ex=ttl_seconds)
    )


async def extend_message_lock(
    redis: Redis, message_id: str, token: str, ttl_seconds: int
) -> bool:
    """Keep the lock alive for another ttl_seconds, but only if `token`
    is still the current holder. Called once per retry attempt (before
    the backoff sleep) so a worker that's genuinely still alive and
    retrying never loses its lock to a reclaim mid-backoff; a worker
    that crashed simply stops renewing, and the lock expires on its own
    (belt-and-braces recovery — a dead worker's message is never stuck
    forever, just delayed by up to ttl_seconds beyond when Streams
    itself would have made it eligible for reclaim)."""
    result = await redis.eval(
        _EXTEND_LOCK_SCRIPT, 1, _message_lock_key(message_id), token, ttl_seconds
    )
    return bool(result)


async def release_message_lock(redis: Redis, message_id: str, token: str) -> None:
    """Best-effort early release once a message is fully processed
    (stored-and-acked, or dead-lettered-and-acked), so the lock doesn't
    sit around for the rest of its TTL doing nothing. Not required for
    correctness — the TTL alone guarantees the lock eventually frees
    itself — just tidiness."""
    await redis.eval(_RELEASE_LOCK_SCRIPT, 1, _message_lock_key(message_id), token)


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


async def stream_lag(redis: Redis) -> int | None:
    """How many entries are sitting in telemetry.raw that this consumer
    group hasn't been delivered yet — XINFO GROUPS' own `lag` field, the
    same number `redis-cli XINFO GROUPS telemetry.raw` shows an operator
    by hand. Used to drive the backlog gauge in worker/metrics.py
    (docs/IMPROVEMENTS.md, "real observability") rather than recomputing
    it some other way — one definition of "how far behind", used both by
    a human at a terminal and by this process's own metrics.

    None (not 0) if the group doesn't exist yet or the server's reply
    doesn't include `lag` (older Redis) — the caller decides what to do
    with "unknown" rather than this function guessing it means zero.
    """
    groups = await redis.xinfo_groups(STREAM_NAME)
    for group in groups:
        if group.get("name") == CONSUMER_GROUP_NAME:
            return group.get("lag")
    return None


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


async def publish_state_update(redis: Redis, payload: str) -> None:
    """Best-effort live feed for GET /api/v1/stream (docs/IMPROVEMENTS.md,
    "live state stream") — called only *after* a message is already
    durably stored and acked (worker/processing.py), so this can never be
    on the path that decides whether a message is safe to ack.

    A publish failure (Redis blip, or Redis briefly unreachable) is
    swallowed here, not raised: nothing durable depends on this, and the
    one rule this module exists to protect (redis_stream.py's own
    module docstring: a message is never acked before it's stored) must
    never be put at risk by an optional side channel. A subscriber that
    misses an update because of this simply sees the next one, or calls
    GET .../latest to catch up.
    """
    try:
        await redis.publish(STATE_UPDATE_CHANNEL_NAME, payload)
    except RedisError as exc:
        logger.warning(
            "telemetry.state_update_publish_failed",
            extra={"event": "telemetry.state_update_publish_failed", "reason": str(exc)},
        )
