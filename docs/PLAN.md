# PLAN — blackbox telemetry backend

Written before any implementation code. Changes during the build are
logged in `docs/PROCESS_LOG.md`.

## 1. Problem framing

**Responsible for:** validating and accepting telemetry over a versioned
API; queueing it so ingest and processing fail/recover independently;
storing every accepted event exactly once; deriving per-asset current
state that never regresses; deriving online/offline from a configurable
last-seen timeout; a query API (assets, latest, bounded history);
liveness vs. readiness; structured, traceable logs; one-command
reproducibility.

**Not responsible for:** generating telemetry (simulator is separate test
equipment); AI/ML/agent logic (excluded by the task); auth; alerting or an
operator UI beyond curl/OpenAPI; cloud deployment; pre-registering asset
ids — new assets are discovered from traffic, no schema change needed.

## 2. Architecture

```
   simulators (test equipment, not part of the product)
   drone-sim   rover-sim   quadruped-sim   [+N more, same contract]
            |  HTTP POST /api/v1/telemetry
            v
   +-------------------------------------------+
   |  api (FastAPI)                              |
   |  ingest: validate, reject before queueing    |
   |  query: assets / latest / history            |
   |  /health/live, /health/ready                 |
   +-------------------------------------------+
            |  XADD                     ^  SELECT
            v                            |
   +-------------------------------------------+
   |  redis stream: telemetry.raw                |
   |  consumer group: worker-group                |
   +-------------------------------------------+
            |  XREADGROUP / XACK / XAUTOCLAIM
            v
   +-------------------------------------------+
   |  worker                                      |
   |  idempotent insert on (asset_id, seq)         |
   |  state advances only if seq is newer          |
   |  bounded-backoff retry on Postgres failure     |
   +-------------------------------------------+
            |  INSERT / UPSERT
            v
   +-------------------------------------------+
   |  postgresql                                  |
   |  telemetry_events (raw, append-only)          |
   |  asset_state (latest snapshot per asset)      |
   +-------------------------------------------+
```

Ingest and query are two routers in one `api` service (D4). Worker is
separate so ingest never depends on worker/Postgres health.

## 3. Data contract

Fields as specified in the brief: `asset_id`, `asset_type`
(drone|rover|quadruped), `seq` (int, monotonic per asset), `timestamp_utc`,
`position.{x_m,y_m,z_m}`, `speed_mps`, `heading_deg`, `battery_pct`,
`temperature_c`, `link_quality_pct`, `mode`.

**Validation (reject, never enqueue, on violation):** all fields required
and correctly typed; `seq >= 0`; `battery_pct`/`link_quality_pct` in
0–100; `heading_deg` in 0–360; `mode`/`asset_type` in their documented
sets; no NaN/Infinity; `timestamp_utc` valid UTC and ≤60s ahead of server
clock (clock-skew allowance). Unknown extra fields are stored, not
interpreted.

**Derived by the backend, never accepted from a client:**
`server_received_at`; online/offline (computed at read time from
`asset_state.last_seen_at` vs. `ASSET_OFFLINE_TIMEOUT_SECONDS`, default
10s — see D3); `asset_state` itself, written only by the worker.

**Identity/ordering:** `(asset_id, seq)` is the idempotency key
(Postgres unique constraint). `seq`, not `timestamp_utc`, decides "newer"
(D1).

## 4. Failure policy

| Case | Policy |
|---|---|
| Malformed / missing fields | `422` with field errors. Never enqueued. |
| Duplicate (`asset_id`+`seq`) | Enqueued anyway; worker insert is `ON CONFLICT DO NOTHING` — one row, no error. |
| Out-of-order / stale (`seq` ≤ stored) | Raw event stored for history; `asset_state` not updated — state only moves forward. |
| Asset goes silent | No event needed — `online` computed on read as `now() - last_seen_at < timeout`. |
| Worker killed/restarted | `XACK` only after commit; unacked messages reclaimed via `XAUTOCLAIM` and safely reprocessed (idempotent insert, monotonic state). |
| Postgres unavailable | No `XACK`; bounded exponential backoff (0.5s→30s cap), logged per attempt; message waits, never dropped. |
| Redis unavailable | Ingest's `XADD` fails → `503` immediately. Never a false accept. |

Known limitation: the Postgres-retry loop assumes transient infra
failure, not a bad message — true poison-message handling is an
exceeds-the-bar item, not baseline.

## 5. Decisions

**D1 — `seq`, not `timestamp_utc`, is the ordering/idempotency authority.**
Rejected: timestamp-based ordering — simulated assets aren't clock-synced,
so it would make "no regression" only probabilistic.

**D2 — Dedup enforced at Postgres, not at the ingest API.**
Rejected: checking Postgres before accepting — would make ingest
availability depend on Postgres, when only Redis should be able to block
ingestion.

**D3 — Online/offline computed at read time, not a stored flag.**
Rejected: a background sweep flipping an `is_online` column — extra
moving part and lag, for a value that's purely `now()` minus one
timestamp.

**D4 — Ingest + query share one `api` service, not two.**
Rejected: separate `ingest-api`/`query-api` services — no load/failure
divergence yet at this scale to justify two more containers.

**D5 — Redis Streams + consumer group, one active worker for now.**
Rejected: pub/sub — no replay, so a down consumer loses messages outright,
violating the no-silent-loss requirement.
