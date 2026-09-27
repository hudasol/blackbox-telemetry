"""Prometheus metric definitions for the api service.

Every metric the api exposes is named here, once, so routers/metrics.py's
`/metrics` endpoint has a single obvious place to point at and no router
has to import `prometheus_client` directly just to bump a counter. See
docs/IMPROVEMENTS.md, "real observability", for why these three and not
more: each one maps to a specific question this project's failure policy
(docs/PLAN.md §4) needs an answer to, not a generic dashboard-filler.

Using the default global `CollectorRegistry` (prometheus_client's module-
level default) rather than a per-app one — this process only ever runs
one FastAPI app (app.main:app), so there's nothing to disambiguate.
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram

# Answers "is ingest actually accepting traffic, and is it rejecting for a
# validation reason (client's fault) or a queue-unavailable reason (this
# system's fault)?" — the three outcomes ingest.py and the 422 handler in
# main.py can produce, plus "replayed" (api/routers/deadletter.py): a
# dead-letter replay's XADD lands on telemetry.raw exactly like a fresh
# POST /api/v1/telemetry does, so it counts as real traffic reaching the
# queue too — without this label, replayed volume was invisible here,
# understating how much traffic telemetry.raw actually received. Kept as
# its own outcome rather than folded into "accepted" so a dashboard can
# still see how much current traffic is fresh ingest vs. replay noise,
# while `sum(rate(...{outcome=~"accepted|replayed"}))` answers "how much
# reached the queue overall".
INGEST_EVENTS_TOTAL = Counter(
    "blackbox_ingest_events_total",
    "Telemetry events reaching the ingest pipeline, by outcome",
    ["outcome"],  # accepted | rejected | queue_unavailable | replayed
)

# Answers "how long does one XADD round-trip actually take, under real
# load?" — not asyncpg latency (that's the worker's), just this service's
# own single dependency for the write path (docs/PLAN.md, D4).
INGEST_LATENCY_SECONDS = Histogram(
    "blackbox_ingest_latency_seconds",
    "Time to enqueue one telemetry event onto Redis (XADD round-trip)",
)
