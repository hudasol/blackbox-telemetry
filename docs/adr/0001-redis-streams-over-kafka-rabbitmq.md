# 0001 — Redis Streams over Kafka/RabbitMQ

## Context

Ingest and processing need to fail and recover independently (`docs/PLAN.md`
§1): a slow or down Postgres must never block `POST /api/v1/telemetry`
from accepting traffic, and a crashed worker must never lose a message
that was already accepted. That requires something durable and
replayable sitting between the two — not a direct call, and not plain
pub/sub (rejected outright in `docs/PLAN.md` D5: a subscriber that's down
when a message is published loses it, which violates the no-silent-loss
requirement on its own).

## Decision

Redis Streams, with one consumer group (`worker-group`) and currently one
active worker consumer.

## Why not Kafka

Kafka's partition model and tooling (schema registry, consumer-lag
dashboards, tiered storage) earn their operational weight once there are
many partitions and many consumers to coordinate. At this project's scale
— one logical stream, one active worker — that weight (a broker cluster,
ZooKeeper or KRaft, topic/partition administration) is pure overhead with
nothing to show for it. Redis is already a hard dependency for nothing
else *but* this queue, so Streams add zero new infrastructure; Kafka would
add an entire second stateful system to operate and back up.

## Why not RabbitMQ

Classic queues give at-least-once delivery too, but replay of
already-delivered-but-unacked work is not the default behavior — it needs
manual-ack mode, explicit dead-lettering, and (for the durability this
project wants) quorum queues, each configured by hand. A Redis Streams
consumer group gives the same shape — a per-consumer pending-entries list,
`XACK`, and `XAUTOCLAIM` to reclaim stale pending messages — as the
out-of-the-box behavior, not an opt-in feature.

## Consequences

- **Single point of failure.** One Redis instance, no Sentinel/Cluster
  here, means a hard Redis failure (not the graceful "service
  unreachable, `503` the caller" case already handled and proven in
  `scripts/fault_injection_demo.sh`'s Postgres analogue) takes the whole
  pipeline down. Acceptable for a portfolio-scale system; not acceptable
  unmodified in production.
- **No built-in partitioning.** One stream, one active consumer: there is
  no per-partition parallelism to lean on if a single worker becomes the
  bottleneck (`scripts/load_test.py` already shows the worker, not
  ingest, is the slower side of the pipeline at the tested load — see the
  README's "Key results").
- **Retention is explicit, not automatic.** The stream itself isn't
  trimmed; `docs/IMPROVEMENTS.md` tracks this as a known gap.
- **Less battle-tested at extreme scale.** Kafka's operational track
  record at very high partition/consumer counts is longer than Redis
  Streams'. See ADR-0004 for what this means at 100x the current load.
