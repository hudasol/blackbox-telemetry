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
    return min(BACKOFF_INITIAL_SECONDS * (BACKOFF_MULTIPLIER**attempt), BACKOFF_MAX_SECONDS)


def _parse_received_at(fields: dict) -> datetime:
    raw = fields.get("received_at")
    if raw:
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            pass
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
        # Should never happen — the ingest API already validated this
        # payload before it was ever enqueued. If it does (a hand-crafted
        # stream entry, or a future incompatible contract change), retrying
        # forever would just wedge the consumer group. Log loudly and move
        # on rather than build full dead-letter handling here — see
        # docs/PLAN.md's known-limitations note.
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

    # Shutting down mid-retry: leave the message unacked. It stays in the
    # consumer group's Pending Entries List and gets picked up by
    # claim_stale_pending on the next startup — never silently dropped.
    logger.info(
        "telemetry.processing_deferred_on_shutdown",
        extra={
            "event": "telemetry.processing_deferred_on_shutdown",
            "asset_id": event.asset_id,
            "seq": event.seq,
            "message_id": message_id,
        },
    )
