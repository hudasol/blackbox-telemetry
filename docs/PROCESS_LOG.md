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
