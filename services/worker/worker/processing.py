"""Per-message processing: parse, store (idempotent + monotonic), ack.

The one rule this module exists to enforce: a message is never acked
before it's durably stored, and never silently dropped on failure — it's
retried with bounded backoff instead (docs/PLAN.md, failure policy).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import asyncpg
from pydantic import ValidationError
from telemetry_contract import TelemetryEvent

from . import db
from .redis_stream import ack

logger = logging.getLogger("worker.processing")

BACKOFF_INITIAL_SECONDS = 0.5
BACKOFF_MAX_SECONDS = 30.0
BACKOFF_MULTIPLIER = 2.0


def next_backoff_seconds(attempt: int) -> float:
    """attempt is 0-indexed (first retry is attempt=0)."""
    return min(
        BACKOFF_INITIAL_SECONDS * (BACKOFF_MULTIPLIER**attempt), BACKOFF_MAX_SECONDS
    )


def _parse_received_at(fields: dict) -> datetime:
    raw = fields.get("received_at")
    if raw:
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            pass
    # Missing/unparseable is itself a sign of a malformed message, but a
    # timestamp is required to store the row — fall back rather than fail
    # the whole message over a field that isn't the point of this check.
    return datetime.now(timezone.utc)


async def process_message(
    redis,
    pool: asyncpg.Pool,
    message_id: str,
    fields: dict,
    shutdown_event: asyncio.Event,
) -> None:
    try:
        event = TelemetryEvent.model_validate_json(fields["payload"])
    except (ValidationError, KeyError, ValueError) as exc:
        # This should never happen — the ingest API already validated the
        # event before enqueueing it. If it does (a bug, a schema change,
        # a hand-crafted stream entry), it's a poison message: retrying it
        # forever would just wedge the stream on something that can never
        # succeed. Full dead-letter handling (inspect/replay) is an
        # exceeds-the-bar item; for now this is logged loudly and acked
        # so it doesn't block everything behind it — see docs/PLAN.md,
        # "known limitation".
        logger.error(
            "telemetry.payload_unparseable",
            extra={
                "event": "telemetry.payload_unparseable",
                "message_id": message_id,
                "reason": str(exc),
            },
        )
        await ack(redis, message_id)
        return

    server_received_at = _parse_received_at(fields)
    attempt = 0

    while not shutdown_event.is_set():
        try:
            result = await db.store_event(pool, event, server_received_at)
        except (asyncpg.PostgresError, OSError) as exc:
            delay = next_backoff_seconds(attempt)
            logger.warning(
                "telemetry.db_retry",
                extra={
                    "event": "telemetry.db_retry",
                    "asset_id": event.asset_id,
                    "seq": event.seq,
                    "message_id": message_id,
                    "attempt": attempt,
                    "delay_s": delay,
                    "reason": str(exc),
                },
            )
            attempt += 1
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            continue

        logger.info(
            "telemetry.stored",
            extra={
                "event": "telemetry.stored",
                "asset_id": event.asset_id,
                "seq": event.seq,
                "message_id": message_id,
                "outcome": result.outcome,
                "attempts": attempt,
            },
        )
        await ack(redis, message_id)
        return

    # Shutdown was requested mid-retry: leave the message unacked. It's
    # still in the Pending Entries List and will be reclaimed by
    # claim_stale_pending on the next startup — not lost, not duplicated.
    logger.info(
        "telemetry.processing_deferred_on_shutdown",
        extra={
            "event": "telemetry.processing_deferred_on_shutdown",
            "asset_id": event.asset_id,
            "seq": event.seq,
            "message_id": message_id,
        },
    )
