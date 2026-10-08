# 0003 — At-least-once delivery, not exactly-once

## Context

The worker only calls `XACK` after the Postgres insert/upsert transaction
commits (`docs/PLAN.md` §4). If the process dies in the gap between
"transaction committed" and "XACK sent," Redis still considers the
message unacked and will hand it to another consumer (or the same one,
restarted) via `XAUTOCLAIM`. That message gets processed again.

## Decision

Accept at-least-once delivery as the end-to-end guarantee, and push all of
the work of making that safe onto the idempotent insert and monotonic
state update described in ADR-0002 — rather than trying to make delivery
itself exactly-once.

## Why not exactly-once delivery

Making delivery itself exactly-once across two independent systems (Redis
and Postgres) means either a distributed transaction or an outbox-pattern
rewrite of the worker — both real engineering, both with their own new
failure modes to reason about and test. That cost buys a guarantee the
idempotent insert already provides for free: the *effect* of processing a
message twice is identical to processing it once. Exactly-once delivery
was rejected because exactly-once *effect* was the actual requirement,
and the cheaper mechanism already satisfies it.

## Consequences

- Every consumer of this pipeline downstream of Postgres can rely on "no
  duplicate rows, state never regresses" without needing to know delivery
  is at-least-once underneath. That's the whole point.
- The Postgres-retry loop (bounded exponential backoff, 0.5s → 30s cap,
  doubling) assumes the failure it's retrying is transient
  infrastructure trouble. A message that is valid JSON matching the
  contract but can never be inserted for some other reason would retry
  forever — the same known gap flagged in ADR-0002, repeated here because
  it's specifically a consequence of choosing retry-until-success over a
  bounded-attempts-then-dead-letter policy at the insert layer.
- This decision is expected to hold even as the system scales past one
  worker replica (see ADR-0004): `XAUTOCLAIM` reclaims messages
  group-wide, not per-instance, so adding more worker replicas doesn't
  change the delivery guarantee, only how many consumers are racing to
  claim stale work.
