# Process log

Running log of framing, decisions, blockers, and changes. Newest entry at
the bottom.

---

## 2026-09-22 — Repository setup + plan

- Repo scaffolded (structure only, no logic) so the plan could be the
  first meaningful commit, per the brief.
- Wrote `docs/PLAN.md`: problem framing, architecture, data contract,
  failure policy, and five decisions (D1–D5) — brief asked for three.
- Blocker: raw `git push` was rejected by the sandbox's egress proxy
  (repo not yet authorized). Resolved by pushing through the connected
  GitHub app instead.
- User asked for `PLAN.md` to be trimmed — brief says it should be short.
  Cut ~40% of the prose while keeping all five required sections.

## 2026-09-23 — Shared telemetry contract

- Added `libs/telemetry_contract`: an installable package (`TelemetryEvent`,
  `Position`, `AssetType`, `AssetMode`, shared constants) so api, worker,
  and simulator import one definition instead of three copies drifting
  apart.
- Validation matches PLAN.md exactly: seq >= 0, battery/link 0–100,
  heading 0–360, enum checks, no NaN/Infinity, 60s future clock-skew
  limit. A naive timestamp is treated as UTC, not local time.
- 27 unit tests in `tests/unit/test_telemetry_contract.py`, run against
  an editable install. One test bug caught in the process: it used
  `datetime.now()` assuming that's UTC — the sandbox actually runs in
  UTC+4, so the model correctly rejected it as 4 hours in the future.
  Fixed the test, not the model.

## 2026-09-23 — Ingest API

- Added `services/api`: `POST /api/v1/telemetry` validates against the
  shared contract, then `XADD`s to Redis (`telemetry.raw`, name now in
  `telemetry_contract.constants` alongside `worker-group`, so worker
  can't drift on either name).
- Structured JSON logging (hand-rolled formatter, no new dependency) —
  every accept/reject logs asset_id, seq, and a request id.
- Real bug caught by smoke-testing with curl (not just pytest): a
  rejection coming from the clock-skew `@model_validator` — as opposed to
  a plain field-type error — carries the raw `ValueError` under
  `ctx.error` in pydantic's error output. Passing that straight into
  `JSONResponse` crashed the handler with an unhandled `TypeError`
  ("not JSON serializable"), so a malformed-in-a-specific-way request
  500'd instead of getting a clean 422. Fixed by stripping `ctx` (the
  `msg` field already has the readable text) and running everything
  through `jsonable_encoder` as a backstop. Added a regression test for
  this exact path, since the other rejection tests all happened to hit
  plain field errors and wouldn't have caught it.
- Tested against a real Redis (not mocked) — 9 integration tests, plus
  the 27 contract unit tests, 36 passing total.
- Note: this sandbox's Docker daemon can't start (`ulimit` operation not
  permitted — nested containers are restricted here), so local testing
  used `redis-server` directly instead of a Redis container. Docker
  Compose itself will be tested properly once that step is reached; if
  the sandbox still can't run a daemon then, the compose file will need
  validating some other way (e.g. `docker compose config`, or handing it
  to the user to run) and that limitation will be documented in the
  README.

## 2026-09-23 — Worker

- Added `services/worker`: consumer-group reader (`XREADGROUP`) for
  `telemetry.raw`, writing to two Postgres tables — `telemetry_events`
  (append-only history) and `asset_state` (one row per asset, latest
  snapshot). No Alembic; schema is `CREATE TABLE IF NOT EXISTS`, run
  idempotently at startup.
- Idempotency and ordering are both a single atomic SQL statement, not
  read-then-write: insert uses `ON CONFLICT (asset_id, seq) DO NOTHING`;
  the state upsert uses `ON CONFLICT (asset_id) DO UPDATE ... WHERE
  asset_state.latest_seq < EXCLUDED.latest_seq`, so a stale/out-of-order
  event can never regress the latest-known state, per PLAN.md's D1 (seq
  is authority, not timestamp).
- Restart recovery: every loop iteration first claims stale pending
  entries (`XAUTOCLAIM`, idle > 30s) before reading new ones, so a
  message a crashed worker read but never finished — whether or not it
  had already reached Postgres — gets picked up by whichever consumer
  restarts next. A message already stored but not yet acked is safe to
  reprocess: the same `ON CONFLICT DO NOTHING` makes it a no-op, and it
  finally gets acked.
- Per-message Postgres failures and stream-level Redis failures both use
  the same bounded exponential backoff (0.5s, doubling, capped at 30s),
  and both are interruptible mid-sleep by shutdown so a SIGTERM during a
  retry doesn't hang — the unacked message is left for the next restart
  to reclaim rather than lost.
- A payload that fails to parse (should never happen — ingest already
  validated it) is logged and acked immediately instead of retried
  forever; documented in PLAN.md as a known limitation, not silent data
  loss.
- Real bug caught by live smoke-testing (not the pytest suite): redis-py
  enforces its own client-side socket read timeout independently of a
  command's `BLOCK` argument, and defaults to 5s regardless of what
  `socket_timeout` you don't pass. A blocking `XREADGROUP ... BLOCK
  5000` reliably raised a spurious `TimeoutError` every ~5s with Redis
  completely healthy and just legitimately idle — would have made the
  worker crash-loop constantly in any real deployment. None of the
  integration tests caught it because test payloads are always
  available immediately, so the blocking wait path never actually
  blocked. Fixed by passing `socket_timeout` explicitly, well above the
  `BLOCK` duration. Also wrapped the main loop's Redis calls in the same
  bounded-backoff pattern as the per-message retries, so a transient
  Redis-level error no longer kills the process either.
- Tested against real Postgres and Redis, not mocks: 4 unit tests
  (backoff math), 8 db tests (every ordering/duplicate scenario), 5
  processing tests (valid, malformed/poison, duplicate, db-failure-then-
  recovery, shutdown-mid-retry), 2 restart-recovery tests (message
  never processed; message stored but not yet acked when the crash
  hit) — 54 passing total across the repo.
- Live demo beyond the automated suite, matching the brief's stated
  acceptance check: ran real `uvicorn` + `python -m worker.main`
  processes, sent a 50-event burst, `kill -9`'d the worker mid-flight,
  restarted it. Result: `XLEN`=60, `XPENDING`=0, Postgres row count=60 —
  every event sent across the whole session landed exactly once, with
  zero loss and zero duplication across two separate hard kills.

## 2026-09-23 — Simulator

- Added `services/simulator`: test equipment (docs/PLAN.md, problem
  framing — not part of the product), a fake fleet of drone/rover/quad
  assets that POSTs to the real ingest API over real HTTP, with four
  independently-configurable fault switches (`SIM_DUPLICATE_RATE`,
  `SIM_OUT_OF_ORDER_RATE`, `SIM_BAD_DATA_RATE`, `SIM_SILENCE_RATE`, each
  a 0.0–1.0 probability per tick, 0 = off) plus `SIM_FORCE_SILENT_ASSET_IDS`
  for a deliberate, noise-free way to demo one asset going offline.
- Split into pure, testable pieces on purpose: `assets.py` (per-tick
  physics — bound-safe, contract-valid by construction), `faults.py`
  (a seeded `FaultInjector` deciding *whether* to duplicate/reorder/
  corrupt/go silent, and *how* to corrupt — 8 corruption kinds, each
  checked against the real `TelemetryEvent` to guarantee it's a 422, not
  a lucky pass), `scheduler.py` (`plan_tick`: pure function turning one
  tick into "what to actually send", no I/O), `client.py` (the only part
  that touches the network). `main.py`'s run loop is thin glue over
  these — one asyncio task per asset, same SIGTERM/SIGINT graceful-
  shutdown pattern as the worker.
- Out-of-order is implemented as hold-then-release: a reorder roll holds
  the current (newer-seq) event and sends it *before* releasing the
  previously-held (older-seq) one next tick — so it's genuinely delivered
  out of order on the wire, not just delayed.
- A single `SIM_SEED` derives per-asset child seeds for both the physics
  and the fault rolls, so a whole run's exact fault sequence is
  reproducible.
- 27 unit tests (physics bounds/contract-validity, fault-rate accuracy
  via seeded sampling, every corruption kind independently proven to
  fail `TelemetryEvent` validation, scheduler decisions for all four
  switches) plus 4 integration tests driving the scheduler + HTTP client
  against a real in-process ingest API and real Redis (proving the
  actual wire behavior, not just the pure logic) — 85 passing total
  across the repo.
- Live end-to-end demo: ran real `uvicorn` + `worker.main` +
  `simulator.main` processes together for 12s with all four switches
  turned up (30% duplicate/out-of-order/bad-data, 10% random silence,
  rover-01 forced fully silent) — 89 events attempted, 60 accepted, 29
  rejected 422 and never enqueued; worker outcomes were 33
  `stored_and_advanced`, 16 `duplicate_ignored`, 11 `stored_but_stale`
  (33+16+11 = 60, matching Redis `XLEN`); `telemetry_events` ended at 44
  rows (33+11 — duplicates correctly created zero extra rows);
  `asset_state.latest_seq` for drone-01 and quad-01 matched their true
  highest seq despite the reordering; rover-01 never appeared anywhere,
  as forced. No bugs found this step — the worker/API code paths this
  exercises were already covered by the previous two steps' fixes.

## 2026-09-23 — Query API + health endpoints

- Added the query router to the existing `api` service (docs/PLAN.md,
  D4): `GET /api/v1/assets` (all known assets), `GET
  /api/v1/assets/{id}/latest` (one asset's current snapshot, 404 if
  unknown), `GET /api/v1/assets/{id}/history?limit=` (bounded raw
  history, newest first, default 50/max 500 — an asset with no history
  is an empty 200, not a 404, since that's a valid answer rather than a
  missing resource). `online` is computed at read time in SQL
  (`EXTRACT(EPOCH FROM (now() - last_seen_at)) < timeout`), never
  stored, per D3.
- Added `GET /health/live` (process up, zero dependency checks — never
  restart a healthy process over a Redis/Postgres blip) and `GET
  /health/ready` (pings both Redis and Postgres, 503 if either is
  unreachable, names which one in the response).
- api now also holds a Postgres pool (`asyncpg`, alongside the existing
  Redis client) since query needs it. Created with `min_size=0`
  deliberately: ingest's Redis dependency and query's Postgres one are
  independent (D4), so a Postgres outage at startup must not stop this
  process from accepting telemetry — connections open lazily on first
  real use instead of eagerly at pool creation.
- Also redacted credentials out of `redis_url`/`postgres_dsn` before
  they reach the startup log line (noticed while adding the postgres_dsn
  log field — the existing redis_url one had the same latent issue).
- Bug found and fixed while wiring this up (not live-testing yet — just
  reading my own code back): the global 422 handler hard-coded
  `"error": "invalid_telemetry_event"` for every validation failure,
  written back when the only thing that could 422 was a bad ingest body.
  It's a global exception handler, though, so a bad `?limit=` on the new
  history endpoint got the same misleading label. Fixed by checking
  `loc[0] == "body"` to tell an ingest rejection from any other kind;
  added a regression test.
- Real bug found live, the same way as the last two steps — running
  actual processes and hitting them, not just pytest: mid-demo, an
  unrelated `pytest` run against the same throwaway Redis instance
  flushed it (its fixtures call `flushdb`), deleting the stream and
  consumer group out from under the *running* worker. That exposed a
  real gap, not just a testing mistake: `ensure_consumer_group` only
  ever ran once, at worker startup, so a `NOGROUP` error (stream/group
  gone, e.g. a Redis restart without persistence or an operator flush)
  retried into the bounded backoff forever and never actually recovered,
  even once Redis was healthy again. Fixed in `worker/main.py`: on a
  `NOGROUP` `ResponseError` specifically, recreate the consumer group
  and retry immediately instead of backing off; a genuine "Redis is
  still down" case falls through to the normal backoff unchanged.
  Verified with an integration test that runs the real `run()` loop,
  deletes the stream mid-flight, and confirms a subsequent event still
  gets stored — and again live, deleting the stream against a live
  worker and watching it self-heal and pick the next event up.
- 11 new integration tests for query/health (against real Postgres +
  Redis, including deliberately-unreachable ones for the two 503 health
  cases) plus 1 for the consumer-group self-heal — 97 passing total
  across the repo.
- Live end-to-end check beyond pytest: ran api + worker + simulator
  together, confirmed `/health/live` stays 200 with both dependencies
  reachable, `/health/ready` reports both `ok`, `/api/v1/assets` and
  `/latest` matched what the worker had just written (including 404 for
  an asset that was forced silent and so never has a row), `history`
  returned events newest-first bounded to the requested limit, and the
  offline timeout correctly flipped `online` to `false` after no events
  for longer than `ASSET_OFFLINE_TIMEOUT_SECONDS`.
