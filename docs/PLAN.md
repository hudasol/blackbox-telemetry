# PLAN — blackbox telemetry backend

Status: written before any implementation code. This is the design the
build will follow; changes to it during implementation are recorded in
`docs/PROCESS_LOG.md`, not silently made here.

## 1. Problem framing

**Responsible for:**

- Accepting telemetry events from any number of simulated assets over a
  versioned HTTP API, and never acknowledging an event it did not durably
  accept.
- Queueing accepted events so ingestion and processing can fail and recover
  independently.
- Persisting every accepted raw event exactly once, even if it was submitted
  more than once.
- Deriving each asset's current state from its event history, in a way that
  cannot be moved backwards by an old or delayed event.
- Deriving asset health (online/offline) from a configurable last-seen
  threshold — not a fixed device registry.
- Exposing a query API for asset list, latest state, and bounded history.
- Reporting liveness (is the process alive) separately from readiness (can
  it actually do its job right now).
- Producing structured logs that let one event be traced from receipt to
  its final outcome (stored / rejected / deduplicated / retried).
- Being reproducible from a clean checkout with one documented command.

**Deliberately not responsible for:**

- Generating telemetry. The simulator is test equipment that talks to the
  same public contract as a real asset would; it is not part of the
  production service and the service has no special knowledge of it.
- Any AI/ML/agent/RAG decision logic, by the task's own constraint.
- Authentication/authorization — out of scope per the brief.
- Alerting, notification, or operator UI beyond what's needed to demo the
  API (curl/OpenAPI docs are sufficient).
- Cloud deployment, multi-region concerns, or horizontal autoscaling —
  local Docker Compose is the only target environment.
- Knowing about asset ids in advance. Assets are discovered from traffic,
  not pre-registered, so adding a fourth or fifth asset never requires a
  schema or code change.

## 2. Architecture

```
   simulators (test equipment — not part of the product)
   drone-sim   rover-sim   quadruped-sim   [+N more, same contract]
            |  HTTP POST /api/v1/telemetry
            v
   +-----------------------------------------------------+
   |  api  (FastAPI)                                       |
   |  - ingest: validates against the telemetry contract,   |
   |    rejects malformed input before it is ever queued    |
   |  - query: reads assets / latest / history from Postgres|
   |  - /health/live, /health/ready                         |
   +-----------------------------------------------------+
            |  XADD (only on ingest path)         ^  SELECT (query path)
            v                                      |
   +-----------------------------------------------------+
   |  redis stream: telemetry.raw                          |
   |  consumer group: worker-group                          |
   +-----------------------------------------------------+
            |  XREADGROUP / XACK / XAUTOCLAIM
            v
   +-----------------------------------------------------+
   |  worker                                                |
   |  - idempotent insert keyed on (asset_id, seq)           |
   |  - updates derived state only if seq is newer           |
   |  - bounded-backoff retry on Postgres failure             |
   +-----------------------------------------------------+
            |  INSERT / UPSERT
            v
   +-----------------------------------------------------+
   |  postgresql                                            |
   |  telemetry_events  — raw accepted events, append-only   |
   |  asset_state       — one row per asset, latest snapshot |
   +-----------------------------------------------------+
```

Ingest and query are two routers inside one `api` service, not two separate
deployables (see decision D4). The worker is its own service so ingest
availability never depends on worker or Postgres health.

## 3. Data contract

**Accepted event fields** (from the brief, unchanged):

`asset_id` (string), `asset_type` (enum: `drone`|`rover`|`quadruped`), `seq`
(integer), `timestamp_utc` (datetime), `position.{x_m,y_m,z_m}`,
`speed_mps`, `heading_deg`, `battery_pct`, `temperature_c`,
`link_quality_pct`, `mode` (string: `idle`|`moving`|`returning`|`charging`|
other documented values).

**Validation rules (reject, do not enqueue, on any violation):**

- All fields above are required and must be the documented type; unknown
  extra fields are accepted and preserved (raw payload is stored as-is) but
  never interpreted.
- `seq`: integer, `>= 0`.
- `battery_pct`, `link_quality_pct`: numeric, `0–100` inclusive.
- `heading_deg`: numeric, `0–360`.
- `mode`, `asset_type`: must be in their documented set.
- No field may be `NaN`/`Infinity`, or a type that doesn't match the
  contract (e.g. a string where a number is required).
- `timestamp_utc`: valid ISO-8601 UTC, and not more than 60s ahead of the
  server's clock (clock-skew allowance; catches obviously broken asset
  clocks without policing normal network latency).

**Fields derived by the backend (never accepted from the client):**

- `server_received_at` — set by the API at ingest time.
- Online/offline status — computed at query time from `asset_state.last_seen_at`
  vs. `ASSET_OFFLINE_TIMEOUT_SECONDS` (env-configurable, default `10`).
  Not stored as a boolean (see decision D3).
- `asset_state` itself (latest snapshot) — written only by the worker,
  never by a client.

**Identity and ordering:** `(asset_id, seq)` is the idempotency key — a
Postgres unique constraint on that pair is what makes "same event received
twice" a no-op instead of a second row. `seq` (not `timestamp_utc`) is the
sole ordering authority for "is this event newer than what we have" (see
decision D1).

## 4. Failure policy

| Case | Policy |
|---|---|
| Malformed / missing fields | Ingest API rejects with `422` and a field-level error body. Never enqueued. |
| Duplicate (`asset_id`+`seq` seen before) | Ingest still enqueues (it doesn't check Postgres). Worker insert is `ON CONFLICT (asset_id, seq) DO NOTHING`. Result: one row, idempotent, no error surfaced to the client that sent it twice. |
| Out-of-order / stale (`seq` ≤ currently stored `seq` for that asset) | Raw event is still stored (history stays complete). `asset_state` is **not** updated — current state only moves forward on `seq >` stored `seq`. Same rule handles both "arrived late" and "arrived out of order." |
| Asset goes silent | No event needed to mark it offline — `online` is computed on read as `now() - last_seen_at < timeout`. Threshold is `ASSET_OFFLINE_TIMEOUT_SECONDS`, read from env, not hard-coded. |
| Worker killed and restarted | Consumer-group semantics: a message is only `XACK`'d after the Postgres write commits. Unacked messages stay in the Pending Entries List and are reclaimed (`XAUTOCLAIM`) and reprocessed. Reprocessing is safe because the insert is idempotent and the state update is monotonic — redelivery cannot duplicate a row or move state backwards. |
| Postgres temporarily unavailable | Worker does not `XACK`. It retries the same message with bounded exponential backoff (0.5s → 30s cap), logging each attempt with the reason. The message is never dropped; it waits in the stream until Postgres recovers. |
| Redis unavailable | Ingest API's `XADD` fails and the endpoint returns `503` immediately. It never returns `2xx` for data it could not durably hand off — this is the one rule the whole ingest path is built around. |

Known limitation (documented on purpose, not hidden): the Postgres-retry
loop above assumes the failure is transient infra, not a bad message. A
message that fails processing for a non-infra reason (a bug, not a down
dependency) would retry the same way today. Distinguishing "wait for
recovery" from "this message is poison" is the dead-letter path listed
under exceeds-the-bar, not part of the meets-the-bar baseline.

## 5. Decisions

**D1 — `seq` is the sole ordering/idempotency authority, not `timestamp_utc`.**
Rejected: using `timestamp_utc` to decide "newest" and to detect duplicates.
Rejected because independent simulated assets have no synchronized clock,
so timestamps can arrive non-monotonically even when the asset's own event
counter is perfectly ordered; basing correctness on an unsynchronized clock
would make the "no regression" guarantee only probabilistically true.

**D2 — Deduplication is enforced at persistence (Postgres unique
constraint), not at the ingest API.**
Rejected: having the ingest API check Postgres (or Redis) for an existing
`asset_id`+`seq` before accepting. Rejected because it would make ingest
availability depend on Postgres availability, which directly contradicts
the requirement that only Redis being down should make ingestion fail —
Postgres being down should be a worker-side, recoverable condition, not an
ingest-blocking one.

**D3 — Online/offline is computed at query time from `last_seen_at`, not
stored as a boolean flipped by a background job.**
Rejected: a periodic sweep that updates an `is_online` column. Rejected
because it adds a moving part (a scheduler) and a lag between an asset
actually going silent and the system reflecting it, for a value that's
strictly a function of `now()` and one timestamp — computing it on read is
always exactly correct and needs no extra process.

**D4 — Ingest and query live in one `api` service (two routers), not two
separately deployed services.**
Rejected: splitting them into `ingest-api` and `query-api` from the start.
Rejected because at this fleet's scale (three assets, ~1 event/s each) they
don't have meaningfully different load or failure profiles yet, and
splitting adds two more containers and health checks for no present
benefit. They don't share mutable state beyond Postgres, so splitting later
if load profiles diverge is a low-cost change, not a rewrite.

**D5 — Redis Streams with a consumer group, one active worker consumer for
now, rather than Redis pub/sub or a naive polling loop.**
Rejected: Redis pub/sub. Rejected outright because it has no persistence or
replay — a consumer that is down when a message is published loses that
message permanently, which fails the "worker restart must not silently
lose data" requirement by construction, not just in an edge case.
