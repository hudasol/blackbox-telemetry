# Architecture decision records

Short, standalone records of the decisions that shape this system. Each one
states the context, what was decided, what was explicitly rejected and why,
and what it costs. These expand on the five numbered decisions in
[`docs/PLAN.md`](../PLAN.md) §5 (D1–D5) rather than replacing them — `PLAN.md`
is the terse original; these are the fuller argument for the three decisions
most worth defending to someone evaluating the system, plus an honest look
at where the current design runs out of road.

- [0001 — Redis Streams over Kafka/RabbitMQ](0001-redis-streams-over-kafka-rabbitmq.md)
- [0002 — Idempotent inserts + monotonic state as the correctness mechanism](0002-idempotent-inserts-and-monotonic-state.md)
- [0003 — At-least-once delivery, not exactly-once](0003-at-least-once-delivery.md)
- [0004 — Known limits today, and what changes at 100x scale](0004-known-limits-and-100x-scale.md)
