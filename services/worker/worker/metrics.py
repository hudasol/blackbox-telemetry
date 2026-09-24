"""Prometheus metric definitions for the worker.

No FastAPI here, so these aren't served by a route — main.py starts
prometheus_client's own tiny WSGI server (start_http_server) in a
background thread instead, the standard way to expose metrics from a
plain asyncio process. See docs/IMPROVEMENTS.md, "real observability".

Four metrics, each answering a question this project's own failure
policy (docs/PLAN.md §4) already promises an answer to — not a generic
dashboard-filler set:
  - is the worker actually processing, and what's happening to each
    message (stored vs. duplicate vs. stale vs. dead-lettered)?
  - how long does one message actually take, end to end?
  - is Postgres flaky right now (the retry-with-backoff path)?
  - how far behind is the worker — is a backlog building or draining?
    (docs/PLAN.md's own "Postgres unavailable" failure case says
    "message waits, never dropped" — this metric is what makes that
    claim observable instead of just asserted.)
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

MESSAGES_PROCESSED_TOTAL = Counter(
    "blackbox_worker_messages_processed_total",
    "Stream messages process_message() has finished handling, by outcome",
    # Mirrors worker.db.StoreResult.outcome, plus "deadlettered" for a
    # message that never reached store_event at all (worker.processing).
    ["outcome"],  # stored_and_advanced | stored_but_stale | duplicate_ignored | deadlettered
)

PROCESSING_LATENCY_SECONDS = Histogram(
    "blackbox_worker_processing_latency_seconds",
    "Time from picking up a message to acking it. Includes DB-retry "
    "backoff sleep on purpose — that sleep is real time a message spends "
    "unprocessed, not overhead to hide from this number.",
)

DB_RETRIES_TOTAL = Counter(
    "blackbox_worker_db_retries_total",
    "Postgres store attempts that failed and were retried with backoff "
    "(worker.processing's db_retry path)",
)

STREAM_BACKLOG = Gauge(
    "blackbox_worker_stream_backlog",
    "telemetry.raw consumer-group lag right now (XINFO GROUPS' own `lag` "
    "field: entries added to the stream but not yet delivered to this "
    "group) — updated once per main loop iteration, not per message.",
)
