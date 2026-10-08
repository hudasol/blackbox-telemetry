# 0002 — Idempotent inserts + monotonic state as the correctness mechanism

## Context

At-least-once delivery (ADR-0003) means the worker will sometimes see the
same message more than once — redelivered after a crash before ack, or
reclaimed by `XAUTOCLAIM` after a restart. The system has to behave
correctly when that happens, not just most of the time.

The natural key for "have I seen this already" is `(asset_id, seq)`, not
`(asset_id, timestamp_utc)`. Timestamps come from the sending asset, and
simulated and real devices alike aren't guaranteed to be clock-synced, so
ordering by timestamp would make "state never regresses" only
probabilistic. `seq` is defined as monotonic per asset by the data
contract itself (`docs/PLAN.md` §3), so it's an authority the backend
doesn't have to trust the clock for (`docs/PLAN.md` D1).

## Decision

Two tables, two different guarantees:

- `telemetry_events` (raw, append-only): `UNIQUE(asset_id, seq)` +
  `INSERT ... ON CONFLICT DO NOTHING`. A redelivered message produces no
  second row and no error — the insert is a true no-op, not a
  caught exception.
- `asset_state` (latest snapshot per asset): updated only when the
  incoming `seq` is strictly greater than what's stored. A redelivered or
  out-of-order message can touch this table and still leave it exactly as
  it was.

Dedup happens at the Postgres layer, not at the ingest API — checking
Postgres before accepting a request would make ingest's availability
depend on Postgres, when only Redis should ever be able to block
ingestion (`docs/PLAN.md` D2). And online/offline status is computed at
read time from `asset_state.last_seen_at`, not stored as a flag kept in
sync by a background sweep — one less moving part, for a value that's
just `now()` minus one timestamp (`docs/PLAN.md` D3).

## Consequences

- The unique constraint is what makes "retry the same insert forever
  until Postgres comes back" (ADR-0003's backoff loop) safe. Without it,
  the retry loop and the dedup goal would be in tension.
- `telemetry_events` grows without bound — there's no retention policy
  yet (tracked in `docs/IMPROVEMENTS.md`).
- A known, documented gap: the Postgres-retry loop assumes the failure is
  transient infrastructure trouble, not a message that is syntactically
  valid but can never be inserted (a true "poison message" at the insert
  layer, as opposed to the parse layer, which is already
  dead-lettered). That message would retry forever under the current
  policy. Not yet fixed; flagged honestly rather than silently left out.
