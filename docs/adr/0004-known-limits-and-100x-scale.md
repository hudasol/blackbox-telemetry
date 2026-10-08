# 0004 — Known limits today, and what changes at 100x scale

## Context

The system is sized for the problem it was actually built to solve: a
handful of simulated assets, one active worker, one Postgres instance.
Honest about where that stops working is more useful than pretending it
scales indefinitely as-is.

## Known limits today

- **One active worker, no internal concurrency.** `worker/main.py`'s read
  loop pulls one message at a time (`count=1`) and processes it fully
  before reading the next. `scripts/load_test.py`'s own numbers (README,
  "Key results") show this: ingest can accept faster than the worker can
  drain, under concurrent load.
- **No connection pool tuning beyond a fixed, small cap.** Both the api
  (`min_size=0, max_size=5`) and the worker (`min_size=1, max_size=5`)
  use a hard-coded small Postgres pool (`services/api/app/main.py`,
  `services/worker/worker/db.py`) — fine at current load, a ceiling that
  hasn't been load-tested past.
- **No rate limiting or auth on ingest.** `POST /api/v1/telemetry`
  accepts from anyone who can reach it, at any rate. Planned as a
  follow-up (`docs/IMPROVEMENTS.md`), not yet built — stated here rather
  than left implicit.
- **No retention on `telemetry_events`.** The raw table grows forever.
  Also tracked in `docs/IMPROVEMENTS.md`.
- **Single Redis instance, single Postgres instance.** No HA/failover on
  either (ADR-0001's consequences, repeated here because it's the same
  root cause: this is a single-node system pretending to be resilient
  only against *process*-level failure, not *node*-level failure).

## What I would change at 100x scale

Roughly in the order they'd start to hurt:

1. **Multiple worker replicas reading the same consumer group.** The
   design already supports this safely (ADR-0002's idempotency, ADR-0003's
   note that `XAUTOCLAIM` is group-wide) — it's a deployment change
   (`docker-compose.yml`/an orchestrator scaling the worker service), not
   a code change, to go from one replica to several.
2. **Partition the stream by `asset_id` hash** once one Redis Streams
   instance can't keep up with aggregate throughput, so parallel workers
   each own a shard instead of all racing over one pending-entries list.
   At that point, the honest comparison with Kafka (ADR-0001) tips the
   other way — Kafka's partition tooling starts earning its operational
   cost.
3. **Read replicas / connection pool sizing for Postgres**, and likely
   splitting the raw-event write path from the query read path onto
   different connections or even different Postgres roles, once query
   traffic and ingest traffic start contending for the same small pool.
4. **A real retention/archival policy** for `telemetry_events` — at 100x
   the event volume, "grows forever" stops being a someday problem.
5. **Redis HA (Sentinel or a managed cluster)** and equivalent Postgres
   failover, once a single-node outage affecting the *whole* fleet
   instead of a redeployable process is a real operational risk, not a
   theoretical one.
