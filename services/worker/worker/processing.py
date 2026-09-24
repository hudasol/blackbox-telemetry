"""Per-message processing: parse, store (idempotent + monotonic), ack.

The one rule this module exists to enforce: a message is never acked
before it's durably stored, and never silently dropped on failure — it's
retried with bounded backoff instead (docs/PLAN.md, failure policy).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone

import asyncpg
from pydantic import ValidationError
from telemetry_contract import TelemetryEvent

from . import db
from .metrics import DB_RETRIES_TOTAL, MESSAGES_PROCESSED_TOTAL, PROCESSING_LATENCY_SECONDS
from .redis_stream import ack, publish_state_update, send_to_deadletter

logger = logging.getLogger("worker.processing")

BACKOFF_INITIAL_SECONDS = 0.5
BACKOFF_MAX_SECONDS = 30.0
BACKOFF_MULTIPLIER = 2.0


def next_backoff_seconds(attempt: int) -> float:
    """attempt is 0-indexed (first retry is attempt=0)."""
    return min(
        BACKOFF_INITIAL_SECONDS * (BACKOFF_MULTIPLIER**attempt), BACKOFF_MAX_SECONDS
    )


def _state_update_payload(event: TelemetryEvent, outcome: str) -> str:
    """The JSON published to STATE_UPDATE_CHANNEL_NAME for GET
    /api/v1/stream (docs/IMPROVEMENTS.md, "live state stream"). The full
    event, not a trimmed-down shape — a live subscriber wants what an
    ingest client sent, same as GET .../history returns. `outcome` is
    added so a subscriber can tell "this asset actually moved forward"
    (stored_and_advanced) apart from an out-of-order event that was
    merely recorded (stored_but_stale) or a resend (duplicate_ignored) —
    the same distinction worker.db.StoreResult already makes.
    """
    return json.dumps({"outcome": outcome, **json.loads(event.model_dump_json())})


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
    started_at = time.monotonic()
    try:
        event = TelemetryEvent.model_validate_json(fields["payload"])
    except (ValidationError, KeyError, ValueError) as exc:
        # This should never happen — the ingest API already validated the
        # event before enqueueing it. If it does (a bug, a schema change,
        # a hand-crafted stream entry), it's a poison message: retrying it
        # forever would just wedge the stream on something that can never
        # succeed, so it still gets acked off telemetry.raw immediately.
        # Unlike the old behavior, it isn't just discarded: the exact
        # original bytes are preserved on a dead-letter stream first, so
        # an operator can see what came in and replay it (as-is, or after
        # a fix elsewhere) instead of it being silently gone forever. See
        # docs/IMPROVEMENTS.md, poison-message handling.
        await send_to_deadletter(redis, message_id, fields, reason=str(exc))
        await ack(redis, message_id)
        MESSAGES_PROCESSED_TOTAL.labels(outcome="deadlettered").inc()
        PROCESSING_LATENCY_SECONDS.observe(time.monotonic() - started_at)
        return

    server_received_at = _parse_received_at(fields)
    attempt = 0

    while not shutdown_event.is_set():
        try:
            result = await db.store_event(pool, event, server_received_at)
        except (asyncpg.PostgresError, OSError) as exc:
            DB_RETRIES_TOTAL.inc()
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
        MESSAGES_PROCESSED_TOTAL.labels(outcome=result.outcome).inc()
        PROCESSING_LATENCY_SECONDS.observe(time.monotonic() - started_at)
        # Only *after* the ack above — this is a best-effort live-view
        # side channel (docs/IMPROVEMENTS.md, "live state stream"), never
        # something the store/ack decision waits on. See
        # publish_state_update's own docstring for why a publish failure
        # is swallowed rather than raised here.
        await publish_state_update(redis, _state_update_payload(event, result.outcome))
        return

    # Shutdown was requested mid-retry: leave the message unacked. Not
    # counted in MESSAGES_PROCESSED_TOTAL — it hasn't finished, and no
    # state update is published either. It's still in the Pending
    # Entries List and will be reclaimed by claim_stale_pending on the
    # next startup — not lost, not duplicated — and reprocessing it then
    # will both count it and publish it at that point instead.
    logger.info(
        "telemetry.processing_deferred_on_shutdown",
        extra={
            "event": "telemetry.processing_deferred_on_shutdown",
            "asset_id": event.asset_id,
            "seq": event.seq,
            "message_id": message_id,
        },
    )
