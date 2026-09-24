# IMPROVEMENTS — beyond the baseline

`docs/PLAN.md` describes the baseline system and its failure policy.
This document covers five additions built on top of it, each addressing
a specific engineering risk the baseline either didn't cover or only
*claimed* to handle without a way to check the claim. Every number below
came from an actual run against the real `docker compose` stack on this
machine (not a bare assertion) — see the "what a real run showed"
subsection under each one for the raw output.

All five keep the baseline's separation of responsibilities intact:
none of them added a new dependency the ingest or worker's core
store-then-ack path relies on, and each section says explicitly how
that was kept true.

## 1. Poison-message handling

**Risk it addresses:** `docs/PLAN.md`'s own failure table listed a known
limitation — the Postgres-retry loop assumes a transient *infra*
failure, not a *bad message*. Before this, a message that could never
parse (a bug, a schema change, a hand-crafted stream entry) would still
be acked off `telemetry.raw` immediately so it didn't wedge every
message behind it, but the bytes were gone the moment that happened.
"Never silently dropped" only half-held.

**What was built:** a second Redis stream, `telemetry.deadletter`
(`libs/telemetry_contract/src/telemetry_contract/constants.py`). On a
parse failure, the worker preserves the exact original message
(`worker/redis_stream.py:send_to_deadletter`) *before* acking it off
`telemetry.raw` — same order as the normal path's
store-before-ack rule, just onto a different stream instead of Postgres.
`GET /api/v1/deadletter` lists what's sitting there (newest first, same
convention as `GET .../history`); `POST
/api/v1/deadletter/{id}/replay` puts one entry back onto
`telemetry.raw` with its original fields and removes it from the
dead-letter stream. Replay goes through the exact same
validate/store/ack path any other message does — no special-cased retry
logic to get wrong. A plain stream rather than a consumer group or a
Postgres table, on purpose: nothing auto-reads it, an operator decides
when to act, and both sides only need Redis Streams commands the worker
and api already use elsewhere.

**Why not the alternatives:** a Postgres "failed_events" table was
rejected — it would mean the worker's write path now depends on two
storage systems succeeding instead of one, for a feature that exists
specifically to handle "something about a message can't be trusted yet."
A consumer group on the dead-letter stream was rejected too — nothing
needs to auto-process a poison message; a human deciding to inspect or
replay one is the whole point.

**How verified:** `tests/integration/test_worker_processing.py`
(dead-letter-on-parse-failure, and a full dead-letter-then-replay
round trip) and `tests/integration/test_deadletter_api.py` (5 tests:
empty listing, newest-first ordering, unparseable-payload display
safety, full replay flow, 404 on an unknown id). Also exercised live
against the running `docker compose` stack:

```
$ docker compose exec -T redis redis-cli XADD telemetry.raw '*' payload 'not valid json'
$ curl -s http://localhost:8000/api/v1/deadletter
{"count": 1, "entries": [{"deadletter_id": "1790278387253-0", ...,
  "reason": "1 validation error for TelemetryEvent\n  Invalid JSON: ...",
  "payload": "not valid json"}]}
$ curl -s -X POST http://localhost:8000/api/v1/deadletter/1790278387253-0/replay
{"status": "replayed", "deadletter_id": "1790278387253-0", "new_message_id": "1790278390773-0"}
$ curl -s http://localhost:8000/api/v1/deadletter
{"count": 1, "entries": [{"deadletter_id": "1790278390774-0", "original_message_id": "1790278390773-0", ...}]}
```

That last response is the honest part: the replayed payload was still
literally `"not valid json"` (replay doesn't fix content, it just gives
something a second chance to succeed), so it failed and was
dead-lettered again — exactly the documented behavior, not a scripted
success.

## 2. Real observability

**Risk it addresses:** `docs/PLAN.md`'s failure table makes several
specific claims — "message waits, never dropped", bounded backoff,
retried until it succeeds. Every one of those claims was previously only
checkable by reading structured logs by hand. There was no way to ask
"is there a backlog building right now" or "how often is Postgres
actually flaky" without grepping.

**What was built:** `prometheus_client`-based metrics, four total, each
chosen to answer one of those specific questions rather than a generic
dashboard-filler set (see the docstrings in `services/api/app/metrics.py`
and `services/worker/worker/metrics.py` for the full reasoning per
metric):

| Metric | Service | Answers |
|---|---|---|
| `blackbox_ingest_events_total{outcome}` | api | is ingest actually accepting traffic, and is a rejection the client's fault (`rejected`) or this system's (`queue_unavailable`)? |
| `blackbox_ingest_latency_seconds` | api | how long does one `XADD` round-trip really take? |
| `blackbox_worker_messages_processed_total{outcome}` | worker | what's actually happening to each message (`stored_and_advanced` / `stored_but_stale` / `duplicate_ignored` / `deadlettered`)? |
| `blackbox_worker_processing_latency_seconds` | worker | how long does one message take end to end, *including* backoff sleep — that sleep is real time a message spends unprocessed, not hidden as overhead |
| `blackbox_worker_db_retries_total` | worker | is Postgres actually flaky right now? |
| `blackbox_worker_stream_backlog` | worker | is a backlog building or draining — the literal thing "message waits, never dropped" needs to be observable to mean anything |

`GET /metrics` on the api (port 8000, same process — it's just another
router). The worker has no HTTP framework, so it runs
`prometheus_client`'s own tiny server on a separate port (9100,
`WORKER_METRICS_PORT`) instead of growing one; a bind failure there is
logged and swallowed, never fatal — losing metrics must never stop the
worker doing its actual job (`worker/main.py`).
`blackbox_worker_stream_backlog` reads `XINFO GROUPS`' own `lag` field
(`worker/redis_stream.py:stream_lag`) — the same number an operator
gets from `redis-cli XINFO GROUPS telemetry.raw` by hand, not a
separately-computed guess.

**How verified:** `tests/integration/test_metrics.py` (6 tests) —
asserts real counter/gauge *deltas* around a real ingest call and a real
`process_message()` call, and that `stream_lag()` correctly reports 3
undelivered messages, then 0 after they're read and acked. Metric names
are process-global, so tests assert deltas, not absolute values — the
only thing a shared-state metric can honestly promise (see that file's
module docstring).

**What a real run showed:** after the fault-injection run below (§3),
the worker's own `/metrics`:

```
blackbox_worker_messages_processed_total{outcome="stored_and_advanced"} 16.0
blackbox_worker_db_retries_total 1.0
blackbox_worker_stream_backlog 0.0
blackbox_worker_processing_latency_seconds_count 16.0
blackbox_worker_processing_latency_seconds_sum 0.5854840589995547
```

16 events processed, exactly one Postgres retry recorded (the one the
kill caused), backlog back to 0 once drained. That db_retries count of
1 — not 8, not 0 — is itself informative: it means Postgres came back
fast enough (`restart: unless-stopped` in `docker-compose.yml`
restarted it almost immediately) that only one message actually hit a
failed attempt before the rest sailed through on the first try. See §3
for why that's not a shortcoming of the test.

## 3. Automated fault injection

**Risk it addresses:** the "Postgres unavailable → message waits, never
dropped" row in `docs/PLAN.md`'s failure table was, before this, a
claim proven only by unit/integration tests that monkeypatch
`db.store_event` to raise — real, but not the same thing as an actual
Postgres process actually dying under a live stack.

**What was built:** `scripts/fault_injection_demo.sh` — `SIGKILL`s the
real `postgres` container in the running `docker compose` stack while
sending deterministic, countable traffic, and checks, against real HTTP
responses and a real row count, that: (a) ingest kept returning `202`
throughout the outage (proving D4 — ingest only depends on Redis,
not Postgres); (b) `/health/ready` correctly reported `503` during the
outage; (c) once Postgres was back, every single event sent — before
and during the outage — ended up stored, with none lost and none
duplicated. It generates its own traffic rather than depending on the
simulator being up, specifically so it has an exact expected count to
check against instead of an approximate one.

**Design note on the outage window:** `docker-compose.yml` sets
`restart: unless-stopped` on `postgres`, so after the kill, Docker
itself restarts the container — a real orchestrator recovering a real
crashed dependency, which is part of what's worth proving, not a
condition to work around. That means the outage window isn't a fixed
duration the script can assume; it confirms the outage is actually
visible via `/health/ready` before trusting that the next block of
requests lands inside it, rather than guessing with a sleep.

**What a real run showed** (`./scripts/fault_injection_demo.sh`):

```
[fault-injection] step 1/6: sending 8 events with Postgres healthy
[fault-injection] step 2/6: SIGKILL-ing the postgres container
[fault-injection] Postgres outage is visible via /health/ready (after 0s)
[fault-injection] step 3/6: sending 8 more events while Postgres is unreachable
[fault-injection] step 5/6: restarting postgres
[fault-injection] api became ready again (after 1s)
[fault-injection] step 6/6: waiting for the worker to drain its backlog and store all 16 events
[fault-injection] all 16 events present in history (after 0s)
[fault-injection] PASS — Postgres outage survived with zero data loss and zero duplication
```

Worker logs from that exact run show the mechanism, not just the
outcome — one retry, then recovery:

```
worker-1  | ... "message": "telemetry.db_retry", "seq": 9, "attempt": 0,
             "delay_s": 0.5, "reason": "[Errno -2] Name or service not known" ...
worker-1  | ... "message": "telemetry.stored", "seq": 9, "outcome": "stored_and_advanced", "attempts": 1 ...
```

Honest limitation: because Docker restarted Postgres almost immediately,
this run only ever hit one failed attempt before recovering — it proves
the retry-then-recover mechanism fires correctly under a real process
death, but it doesn't demonstrate the backoff curve climbing through
several steps toward its 30s cap. Stopping the container instead of
killing it (`docker compose stop postgres` — no auto-restart, an
operator has to bring it back) or temporarily removing `restart:
unless-stopped` would produce a longer, more dramatic outage; deliberately
not done here so the script matches the actual `docker-compose.yml`
this project ships, rather than a version edited just to make a better
demo.

## 4. Performance proof

**Risk it addresses:** the two paths in this pipeline (ingest → Redis,
worker → Postgres) have different jobs, and it would be easy to assume
which one is slower without measuring — e.g. "the worker does a
transactional Postgres write per message, so it must be the
bottleneck." That's a guess, not a fact, until measured.

**What was built:** `scripts/load_test.py` — sends N events at a
configurable concurrency, measures real p50/p95/p99 ingest latency and
throughput, then measures a separate number: how long the worker takes
to fully drain that batch (poll `GET .../history` until the count
matches). Comparing the two numbers is the actual point — it's what
turns "which side is slower" from a guess into a measured fact instead
of assuming the more complex-looking side must be it.

**What a real run showed** (`python3 scripts/load_test.py --count 500
--concurrency 50`, then again at `--concurrency 100`, against the live
stack, 2 CPU cores available on this machine):

| concurrency | ingest throughput | ingest p50 / p95 / p99 | worker drain rate |
|---|---|---|---|
| 50 | 245.4 req/s | 139.5ms / 532.6ms / 860.4ms | 1395.3 events/s |
| 100 | 357.3 req/s | 241.4ms / 353.1ms / 403.9ms | 805.6 events/s |

**The actual finding — not the one assumed going in:** the worker was
*faster* than ingest in both runs, not slower. `worker/main.py`'s loop
reads one message at a time with no internal concurrency, yet still
drained 500 messages in well under a second — a single-transaction
Postgres insert+upsert is cheap. Ingest's own request-handling latency
(single `uvicorn` process, one asyncio event loop, JSON parsing +
Pydantic validation + a Redis round trip per request, all competing for
this sandbox's 2 CPU cores alongside the worker/Redis/Postgres
containers) was the real limit, and it didn't scale linearly with
concurrency — throughput went up 46% (245→357 req/s) but p50 latency
got *worse* (139ms→241ms) going from 50 to 100 concurrent requests,
which is the signature of a CPU-bound single process under contention,
not a system with slack capacity to spare. `docker stats` sampled mid-run
at concurrency 100 showed `api` at ~28% and `worker` at ~43% of this
machine's total CPU — consistent with contention on 2 cores, not one
service starving.

Honest caveat: this ran on a 2-core sandbox with five containers sharing
it, including the load generator's own client process — the absolute
numbers would look different on real hardware or with `uvicorn --workers
N`. What the numbers reliably show, independent of the hardware, is the
*relationship*: this pipeline's bottleneck under load is ingest's
request-handling path, not the worker's Postgres write, and that a
single-process ingest deployment (which is what `docker-compose.yml`
runs today) doesn't scale its latency well past ~50 concurrent
requests on constrained CPU. Scaling `api` (more `uvicorn` workers, or
more replicas behind a load balancer) would be the first lever to pull,
not tuning the worker or Postgres.

## 5. Live state stream

**Risk it addresses:** the query API only supports pull (poll
`GET .../latest` or `.../history` on a timer). A client that wants to
watch an asset's state as it changes has no way to do that without
polling, and polling either wastes requests (poll fast) or lags behind
(poll slow).

**What was built:** `GET /api/v1/stream` — Server-Sent Events, optional
`?asset_id=` filter. The worker publishes to a Redis Pub/Sub channel,
`telemetry.updates`, immediately *after* a message is durably stored and
acked (`worker/processing.py`, right after the existing `ack()` call).
This is the one place in this whole set of additions where "don't
weaken the core reliability work" needed an explicit, deliberate
design choice, not just a comment saying so:

- **Ordering:** publish only happens after ack, never before or
  instead of it — nothing about whether a message is safe to
  acknowledge depends on whether anyone is listening for the live feed.
- **Failure isolation:** a publish failure (a Redis blip) is caught and
  logged inside `publish_state_update`, never raised back into
  `process_message` — it can't fail a message that already succeeded.
- **Primitive choice:** Pub/Sub, not a Stream. A Stream would need
  something to eventually trim it or it grows forever; Pub/Sub simply
  drops a publish with zero subscribers — it physically cannot
  accumulate a backlog, so a slow or absent SSE client creates zero
  memory pressure on Redis or the worker. The trade-off (a subscriber
  that's briefly disconnected misses whatever was published in that
  window) is the right one for a feed that's explicitly a live
  convenience on top of the pull-based query API, not a replacement
  for it — a client that reconnects just calls `GET .../latest` to
  catch up, same as it would on first load.
- **Backpressure:** this router only ever reads the Pub/Sub channel; it
  never touches `telemetry.raw` or the consumer group, so nothing an
  SSE client does can affect ingest or the worker's own reads.

**How verified:** `tests/integration/test_stream_api.py` — drives the
router's generator function directly against real Redis Pub/Sub and the
real worker `process_message()` call site (not a raw `PUBLISH`), so it
proves the actual publish call site works, not just that the router can
read whatever's handed to it. Three cases: a stored event arrives over
the stream with the right outcome; the `asset_id` filter actually
filters (publishes two assets, subscriber only sees the one it asked
for); a stale/out-of-order event is published with its real outcome
(`stored_but_stale`), not misrepresented as an advance. (httpx's
`ASGITransport` — used for every other router's integration tests in
this suite — turned out to buffer a `StreamingResponse`'s entire body
before returning anything, so it can't drive a generator that's
designed to run until the client disconnects; verified this by
reproducing it in isolation, not assumed — see that test file's module
docstring.)

**What a real run showed:**

```
$ curl -N "http://localhost:8000/api/v1/stream?asset_id=sse-live-check" &
$ curl -s -X POST http://localhost:8000/api/v1/telemetry -d '{"asset_id": "sse-live-check", "seq": 1, ...}'
: keep-alive

data: {"outcome": "stored_and_advanced", "asset_id": "sse-live-check", "asset_type": "drone",
       "seq": 1, "timestamp_utc": "2026-09-24T19:33:00Z", ..., "mode": "moving"}
```

The event was sent over HTTP, went through the worker's real
store-then-ack-then-publish path, and arrived on the SSE connection —
the whole chain, live, not simulated.
