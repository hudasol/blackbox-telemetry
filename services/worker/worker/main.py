"""Worker entrypoint.

Every loop iteration first sweeps for work abandoned by a crashed worker
(claim_stale_pending), then reads new messages. That ordering is what
makes a restart actually recover in-flight work rather than just resume
from wherever the stream happens to be (docs/PLAN.md, failure policy).
"""

from __future__ import annotations

import asyncio
import logging
import signal

from prometheus_client import start_http_server
from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from .config import get_settings
from .db import bootstrap_schema, create_pool
from .logging_config import configure_logging
from .metrics import STREAM_BACKLOG
from .processing import next_backoff_seconds, process_message
from .redis_stream import claim_stale_pending, ensure_consumer_group, read_new, stream_lag

logger = logging.getLogger("worker")

# A message idle (read but unacked) longer than this is assumed abandoned
# by a dead consumer, not just slow — reclaimed regardless of who read it.
CLAIM_MIN_IDLE_MS = 30_000
READ_BLOCK_MS = 5_000

# redis-py enforces its own client-side socket read timeout independently
# of a command's BLOCK argument (confirmed empirically — not documented
# clearly). If it's <= READ_BLOCK_MS, a plain "nothing new to read" tick
# raises TimeoutError and looks exactly like Redis being down. Must stay
# comfortably above READ_BLOCK_MS. See docs/PROCESS_LOG.md.
SOCKET_TIMEOUT_SECONDS = (READ_BLOCK_MS / 1000) + 10


async def run(shutdown_event: asyncio.Event) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=SOCKET_TIMEOUT_SECONDS,
    )
    pool = await create_pool(settings.postgres_dsn)
    await bootstrap_schema(pool)
    await ensure_consumer_group(redis)

    # prometheus_client's own tiny WSGI server, in a background thread —
    # no route to wire up, since this process (unlike api) has no HTTP
    # framework of its own (docs/IMPROVEMENTS.md, "real observability").
    # A bound-port failure (in production: something else already on
    # settings.metrics_port; in tests: run() started a second time in
    # the same process — see test_worker_redis_unavailable_recovery.py
    # and test_worker_consumer_group_recovery.py, which both run a real
    # worker.main.run() loop) is logged, not fatal: metrics are
    # observability, and losing them must never stop this process from
    # doing its actual job of processing telemetry.
    try:
        start_http_server(settings.metrics_port)
    except OSError as exc:
        logger.warning(
            "worker.metrics_server_failed",
            extra={
                "event": "worker.metrics_server_failed",
                "port": settings.metrics_port,
                "reason": str(exc),
            },
        )

    logger.info(
        "worker.startup",
        extra={"event": "worker.startup", "consumer_name": settings.consumer_name},
    )

    redis_error_attempt = 0
    try:
        while not shutdown_event.is_set():
            try:
                # Once per iteration (not per message) is enough for a
                # gauge meant to answer "is a backlog building or
                # draining right now", and keeps this off the hot path of
                # every single message.
                lag = await stream_lag(redis)
                if lag is not None:
                    STREAM_BACKLOG.set(lag)

                reclaimed = await claim_stale_pending(
                    redis, settings.consumer_name, CLAIM_MIN_IDLE_MS
                )
                for message_id, fields in reclaimed:
                    logger.info(
                        "telemetry.reclaimed",
                        extra={
                            "event": "telemetry.reclaimed",
                            "message_id": message_id,
                        },
                    )
                    await process_message(
                        redis, pool, message_id, fields, shutdown_event
                    )

                new_messages = await read_new(
                    redis, settings.consumer_name, block_ms=READ_BLOCK_MS
                )
                for message_id, fields in new_messages:
                    await process_message(
                        redis, pool, message_id, fields, shutdown_event
                    )
                redis_error_attempt = 0
            except RedisError as exc:
                # NOGROUP means the stream and/or consumer group is gone
                # out from under us — a Redis restart without persistence,
                # or an operator flush — even though Redis itself is
                # reachable. Retrying the same read forever would never
                # recover on its own; recreate the group (mkstream=True
                # recreates the stream too) and carry straight on instead
                # of waiting out a backoff for something re-creating won't
                # fix by waiting. Found live: docs/PROCESS_LOG.md.
                if isinstance(exc, ResponseError) and "NOGROUP" in str(exc):
                    logger.warning(
                        "worker.consumer_group_missing_recreating",
                        extra={
                            "event": "worker.consumer_group_missing_recreating",
                            "reason": str(exc),
                        },
                    )
                    try:
                        await ensure_consumer_group(redis)
                        redis_error_attempt = 0
                        continue
                    except RedisError:
                        pass  # Redis itself is down too — fall through to backoff.

                # A blip talking to Redis itself (not a per-message
                # Postgres failure, which process_message already handles)
                # shouldn't kill the worker either — same bounded-backoff
                # philosophy as the rest of the failure policy.
                delay = next_backoff_seconds(redis_error_attempt)
                logger.error(
                    "worker.redis_error",
                    extra={
                        "event": "worker.redis_error",
                        "attempt": redis_error_attempt,
                        "delay_s": delay,
                        "reason": str(exc),
                    },
                )
                redis_error_attempt += 1
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
    finally:
        await redis.aclose()
        await pool.close()
        logger.info("worker.shutdown", extra={"event": "worker.shutdown"})


def _install_signal_handlers(shutdown_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)


async def main() -> None:
    shutdown_event = asyncio.Event()
    _install_signal_handlers(shutdown_event)
    await run(shutdown_event)


if __name__ == "__main__":
    asyncio.run(main())
