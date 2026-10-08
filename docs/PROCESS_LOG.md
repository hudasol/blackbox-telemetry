# Process log

Running log of framing, decisions, blockers, and changes. Newest entry at
the bottom.

---

## 2026-09-22 — Repository setup + plan

- Repo scaffolded (structure only, no logic) so the plan could be the
  first meaningful commit.
- Wrote `docs/PLAN.md`: problem framing, architecture, data contract,
  failure policy, and five decisions (D1–D5) — three was the self-set bar.
- Blocker: raw `git push` was rejected by the sandbox's egress proxy
  (repo not yet authorized). Resolved by pushing through the connected
  GitHub app instead.
- User asked for `PLAN.md` to be trimmed to stay short and skimmable.
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
- Live demo beyond the automated suite, matching the documented
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

## 2026-09-23 — Docker Compose

- Added `docker-compose.yml` wiring postgres, redis, api, worker, and
  simulator on one network — `docker compose up --build` is the whole
  setup step, matching the "one-command reproducibility" requirement in
  `docs/PLAN.md`. No manual migration step: schema bootstrap already ran
  at worker startup (`worker/db.py`), so a fresh `pgdata` volume comes up
  usable on its own.
- One `Dockerfile` per service (`services/{api,worker,simulator}/Dockerfile`),
  each built with the repo root as context so it can reach
  `libs/telemetry_contract` and install it separately from the service's
  own package — the same workspace-local pattern as local dev, just
  inside the image instead of a local venv.
- Startup order is enforced with real healthchecks, not fixed sleeps:
  `depends_on: condition: service_healthy` on postgres (`pg_isready`)
  and redis (`redis-cli ping`) before api/worker start; api's own
  healthcheck hits `/health/ready` — not `/health/live` — specifically
  so `simulator`'s `depends_on: api: service_healthy` doesn't let it
  start firing events until api can actually reach both its real
  dependencies over the compose network, not just until the process is
  up.
- `.env.example` documents the few things worth overriding without
  touching the compose file: log level, the offline timeout, and the
  simulator's fault-rate switches. Those switches default to non-zero
  *in compose* (0.1 duplicate/out-of-order/bad-data, 0.05 silence) even
  though the simulator's own bare defaults are all off — so a plain
  `docker compose up` demonstrates every documented failure path
  immediately, not just a clean happy-path fleet.
- postgres's host port is published as 5433, not 5432 — this sandbox
  already had a native Postgres bound to 5432 for the local test suite,
  and 5432 is a common enough default elsewhere that shifting it avoids
  the same collision for anyone else running this alongside another
  Postgres. Internal service-to-service traffic is unaffected either
  way (always `postgres:5432` on the compose network).
- Correction to a limitation documented on 2026-09-23 (Ingest API entry):
  this sandbox's Docker daemon could not be started that day. It starts
  fine now — nothing about the sandbox changed on my end; it was
  presumably a transient restriction on the container runtime that day,
  not a permanent one. Recorded here rather than silently overwriting
  the earlier note, since the earlier entry was accurate for its own
  day and shaped real decisions (local `redis-server`/`postgresql`
  instead of containers for all testing through the Query API step).
- The one thing that could *not* be tested as shipped: this sandbox's
  outbound HTTPS goes through a TLS-intercepting proxy with a
  self-signed CA that the sandbox's host Python trusts (via env vars)
  but a fresh `python:3.12-slim` build container does not, and pip's
  isolated build-dependency subprocess ignores proxy/cert env vars
  regardless (`pip install --isolated` behavior) — so `pip install`
  inside a container here can't reach PyPI at all without sandbox-only
  workarounds (trusting the sandbox's CA bundle, `--no-build-isolation`)
  that would be meaningless noise in the committed Dockerfiles, since
  neither problem exists on a normal network. Worked around it for
  verification only: built temporary local variants of the three
  Dockerfiles with those two accommodations added, `docker build
  --network=host` (to reach the sandbox's loopback-bound proxy) tagged
  to the exact image names `docker compose build` would produce, then
  ran `docker compose up` against those pre-built images so the rest —
  networking, healthchecks, startup ordering, the actual running system
  — was exercised for real. Deleted the temporary Dockerfiles and the
  copied CA bundle before committing; `git status` confirms only the
  intended six files were added.
- Full live run beyond `docker compose config` (which was also checked):
  brought up all five containers, confirmed the healthcheck-gated start
  order (`postgres`/`redis` healthy → `worker`/`api` start → `api`
  healthy → `simulator` starts), then hit every endpoint through the
  published port and watched the default fault rates actually produce
  real `202`/`422` responses and `stored_and_advanced`/`duplicate_ignored`/
  `stored_but_stale` worker outcomes end-to-end over the compose
  network (not just localhost, as every prior live demo had used).
  `docker compose kill -s SIGKILL worker` then `docker compose up -d
  worker` recovered with zero pending messages (`XPENDING` back to 0)
  and no gap in `telemetry_events`, mirroring the worker step's own
  restart-recovery demo but this time through the container runtime.
  `docker compose down` (no `-v`) then `docker compose up -d` again
  confirmed `asset_state`/`telemetry_events` survived on the named
  `pgdata` volume while the Redis stream correctly did not persist
  (no volume mounted for redis) — the worker just recreated the
  consumer group and kept going, `latest_seq` continuing to climb with
  no regression. Cleaned up with `docker compose down -v` afterward.
  No product bugs found this step — the whole point was proving the
  packaging, not the system underneath it, which the last five steps
  had already exercised hard.

## 2026-09-23 — Tests

- Every step so far already carried its own tests as it was built (see
  each entry above) — this step is a deliberate audit, not a first
  pass: line up `docs/PLAN.md`'s failure-policy table, all seven
  documented rows, against what's actually covered, and close whatever
  isn't. Went through it row by row against the 97 tests already in
  place plus a full read of the ingest/query/worker source.
- Five of seven rows had solid coverage already: malformed/missing
  fields (contract unit tests + ingest 422 tests), duplicates (worker
  db tests, worker processing tests, ingest's own "enqueued both times"
  test), out-of-order/stale (worker db tests, including a 1-3-2
  arrival-order test), asset-goes-silent (query API's online/offline
  test), worker killed/restarted (two restart-recovery tests, the
  NOGROUP self-heal test, and three separate live demos across the
  Worker/Simulator/Docker Compose steps).
- Two real gaps found, both closed:
  - Worker's Postgres-unavailable row was tested only through a
    monkeypatched `store_event` failing a couple of times inside
    `process_message` — the *outer* main loop's own generic
    `except RedisError` backoff-and-recover branch (as opposed to its
    NOGROUP-specific sibling, which does have a dedicated test) had
    never actually been pinned down by an automated test. It was only
    ever caught live, and only by accident — the socket_timeout bug
    documented under the Worker step happened to exercise the same
    branch, but that test was never written back once the bug was
    fixed. Added `test_worker_redis_unavailable_recovery.py`: drives
    the real `worker.main.run()` loop, monkeypatches `read_new` to
    raise a plain `RedisError` (deliberately not a NOGROUP
    `ResponseError`, to land in the other branch) three times before
    delegating to the real implementation, and confirms the event
    still gets stored. Mutation-tested it before trusting it: patched
    `main.py` to re-raise instead of backing off, watched the test
    fail with exactly the injected error, then reverted — confirming
    it isn't vacuously passing.
  - The query endpoints' own `query_unavailable` 503 branch
    (`routers/query.py`, `_query_unavailable()`) had zero coverage —
    only `/health/ready`'s parallel-but-separate check was tested. A
    client hitting `/api/v1/assets` (or `/latest`, or `/history`)
    while Postgres is down got no automated proof it degrades the same
    honest way ingest does on a dead Redis, rather than 500ing or
    hanging. Added three tests in `test_query_api.py`, one per
    endpoint, against the same `UNREACHABLE_POSTGRES_DSN` the health
    tests already use.
- Repeated the two new integration tests (plus the existing NOGROUP
  one, since it shares the same background-task-driving pattern) three
  times in a row to rule out timing flakiness before trusting them —
  all stable.
- 101 passing total across the repo (97 + 4 new). No product bugs
  found — every gap here was a coverage gap, not a behavior bug; the
  code underneath was already doing the right thing in both cases, it
  just wasn't provable by the suite until now.

## 2026-09-23 — CI

- Added `.github/workflows/ci.yml`: runs the whole suite on every push
  and every pull request. Real Postgres/Redis as GitHub Actions service
  containers (`postgres:16-alpine`, `redis:7-alpine`) — not mocks, same
  as local dev and every step's own testing — with ports and
  credentials set to exactly match `tests/integration/conftest.py`'s
  own defaults, so the test job needs no DSN/URL overrides beyond
  pointing `localhost` at the mapped ports.
- Install order mirrors local dev and the Dockerfiles exactly: the
  shared `telemetry_contract` first, then each service editable-
  installed with its `[dev]` extra (pytest, pytest-asyncio, httpx) on
  top of its runtime deps — one command per service, no separate
  requirements file to keep in sync.
- Added a second, much cheaper job, `compose-config`: just `docker
  compose config --quiet` against the Docker Compose step's file.
  `ubuntu-latest` ships Docker and Compose v2 preinstalled, so this
  costs nothing beyond a checkout, and catches a broken compose file
  (bad YAML, a missing build context, a typo'd `depends_on` service
  name) on every push without the cost of actually building five
  images — a real build was already proven live in the Docker Compose
  step; this just guards against the file rotting afterward.
- Checked both pinned action versions against what's actually current
  right now rather than trusting training data, since an action's
  major version is exactly the kind of thing that goes stale silently:
  `actions/checkout` is at v5 (was tempted to write v4 from habit) and
  `actions/setup-python` is at v6 — both confirmed via a live search,
  not assumed.
- Verified the whole job locally before trusting it, not just eyeballed
  the YAML: built a completely fresh venv with `python3.12` explicitly
  (the sandbox's bare `python3` is 3.11, which would have silently
  simulated the wrong runner — caught this on the first attempt, since
  pip refused every package with "requires a different Python"),
  installed in the exact order and with the exact commands the
  workflow uses, then ran the suite with the same env vars the
  workflow sets, against this sandbox's real Postgres/Redis. 101
  passed, clean install, no surprises. `docker compose config --quiet`
  re-checked after pinning the action versions, still clean.
- No product bugs — this step doesn't touch product code. The one real
  bug was in my own verification method (simulating with the wrong
  local Python), caught and fixed before it could have produced a false
  "CI will pass" confidence.

## 2026-09-23 — README

- Rewrote `README.md` from the placeholder scaffolding note into real
  setup/run/demo/test instructions, now that every step it describes
  actually exists: one-command `docker compose up --build`, a demo
  walkthrough (health checks, the query endpoints, sending an event by
  hand, killing and restarting the worker mid-stream to show nothing is
  lost), a full API reference table, local dev without Docker, how to
  run the tests, and what CI does — plus an architecture diagram and a
  repo-layout map, both trimmed from/pointing at `docs/PLAN.md` rather
  than duplicating it.
- Every command in the file was actually run, not just written to look
  plausible — this step's whole point is that these instructions have
  to work for someone who wasn't here for the build:
  - Local-dev-without-Docker section: created a throwaway `blackbox`
    Postgres database and a Redis on port 6379 (this sandbox's defaults
    were already occupied by the test infra, on different ports), then
    ran `uvicorn app.main:app`, `python -m worker.main`, and
    `python -m simulator.main` exactly as documented. `/health/ready`,
    `/health/live`, all three query endpoints, and a hand-sent
    `POST /api/v1/telemetry` all returned exactly what the README
    claims; the simulator's own traffic showed up seconds later, and
    the earlier hand-sent event (higher `seq`) correctly stayed the
    latest snapshot once the simulator's lower-`seq` events arrived —
    D1's ordering rule holding up end to end, not just in a unit test.
  - Real bug caught by this: the demo curl example hard-coded
    `timestamp_utc` to a fixed value. It 422'd immediately — the
    contract rejects anything >60s in the future (docs/PLAN.md §3), and
    a hard-coded timestamp is always eventually "in the past enough" or
    was already wrong the moment it was written for "now". Fixed by
    generating it inline with `$(date -u +%Y-%m-%dT%H:%M:%SZ)` instead,
    so the example is correct whenever someone actually runs it, not
    just on the day it was written. Re-ran it after the fix to confirm
    the `202`, then confirmed the event showed up correctly on all three
    query endpoints.
  - Tests section: verified the documented `pytest tests/ -v` both
    against this sandbox's existing native Postgres/Redis test infra
    (101 passed) and, separately, against disposable Docker containers
    on alternate ports standing in for the exact `docker run` commands
    the README tells a fresh clone to use (same image tags, env vars,
    and — critically — the same ports/credentials `conftest.py`
    defaults to and CI's service containers use), to confirm that path
    works too without relying on this sandbox's pre-existing test setup.
    Also 101 passed.
  - `docker compose up --build` itself was not rebuilt from scratch
    again here — hit the same sandbox-only TLS-proxy issue documented
    in the Docker Compose entry (this build container's network can't
    reach the sandbox's loopback-bound proxy) — but that command's
    actual behavior (healthcheck-gated startup order, real
    accept/reject/duplicate outcomes, kill-and-recover) was already
    proven live in that step, so the README's Run/Demo sections
    describe behavior that was verified then, not assumed now.
    `docker compose config --quiet` was re-checked here and is clean.
- Left every touched piece of infrastructure exactly as it was found:
  dropped the throwaway `blackbox` database, killed the port-6379 Redis
  and the manually-started api/worker/simulator processes, removed the
  Docker test containers, and confirmed via `git status` and a port
  check that nothing besides `README.md` changed and the sandbox's
  original test infra (native Postgres on 5432, Redis on 16379) was
  untouched throughout.
- No product bugs. One real bug, in the README's own demo instructions
  (the stale-timestamp example above) — exactly the kind this step
  exists to catch before a reader hits it instead.

## 2026-09-23 — Closing out two process lessons from this build

Asked, after all nine steps were done, for an honest retrospective: did
every step go well, and what would be done differently. Two real
process lessons came out of that — not product bugs, but gaps in how
the work itself was pushed and verified — and rather than leave them as
just talk, went back and closed both.

- **Lesson 1: a push can silently diverge from what was tested.** The
  README step's first push accidentally sent a truncated copy of this
  file (caught and fixed same-step, see above). That raised the
  question of whether anything *earlier* had the same problem and just
  went unnoticed. Checked, properly this time: compared the git blob
  SHA of every one of the 59 tracked files between the local repo and
  GitHub's `main`, not just spot-checked a few. Found three more —
  `services/worker/worker/db.py`, `processing.py`, and
  `redis_stream.py` — out of sync since the original Worker step commit
  itself. Diffed both versions line by line before concluding anything:
  every SQL statement, function body, and return value was identical:
  the only drift was docstrings and explanatory comments added locally
  before that commit was made, that never made it into whatever content
  got pasted into that step's push call. Never a behavior difference —
  nothing running in production ever differed from what was tested —
  but a real gap between "what's in the repo I reviewed" and "what's
  actually on GitHub" all the same. Pushed the correct, fully-documented
  versions of all three files and re-verified their SHAs now match.
  Two harmless leftover `.gitkeep` files (`.github/workflows/.gitkeep`,
  `services/simulator/simulator/.gitkeep`) showed up on GitHub but not
  locally — pre-real-files scaffolding, zero bytes, no effect on
  anything; left alone rather than treated as part of this cleanup.
  The standing rule going forward: never reconstruct a file's content
  for a push from memory or from what's already in context — always
  read it fresh, immediately before that specific push call, no matter
  how recently it was written or how confident the content seems.
- **Lesson 2: the one full `docker compose up --build` proof was
  already a few steps old.** The Docker Compose step proved a genuine
  from-scratch build and run worked, but only once, manually, using a
  sandbox-only workaround for this environment's TLS-intercepting
  proxy (documented under that step) — a proof that goes stale the
  moment a dependency release, a base image update, or a Dockerfile
  edit changes what actually gets built, with nothing to notice if it
  breaks. Closed this permanently rather than just re-running it once
  more: added a third CI job, `docker-smoke`, that runs `docker compose
  up -d --build` for real on every push, polls `/health/ready` until
  the api answers, then confirms `/api/v1/assets` actually shows
  traffic that came from the simulator through the worker into
  Postgres — the same thing the Docker Compose step's live demo proved
  by hand, now proved automatically, forever, on every single push.
  GitHub's runners have a normal network, so this needs none of this
  sandbox's proxy workaround. Verified the exact script (build, health
  poll, traffic check, teardown) locally first, using that same
  sandbox-only workaround one more time for verification only:
  hand-built all three images with the documented `--no-build-isolation`
  / trusted-CA accommodations, tagged to the exact names `docker
  compose build` would produce, then ran the real `docker compose up
  -d` (no rebuild needed, since the tags already matched) followed by
  the exact wait-loop and `/api/v1/assets` check the new CI job uses —
  api became ready on the first poll, three assets showed up with
  climbing `seq` numbers within seconds, logs confirmed the worker
  storing them and the simulator sending them. Deleted the temporary
  images and the copied CA bundle afterward; `git status` and `docker
  images` both confirm nothing sandbox-specific was left behind.
- No product bugs in either fix — both were about the gap between what
  was built/tested and what the repository on GitHub actually reflects
  or re-proves, not about the system's own behavior.

## 2026-09-23 — History endpoint: from/to time-range filtering

`GET /api/v1/assets/{asset_id}/history` only supported "most recent N"
(`limit`). A caller who needs everything between two specific
timestamps — the actual "minimum api" requirement — had no way to ask
for that. Added `from`/`to` query parameters alongside the existing
`limit`, rather than replacing it:

- Both bounds are inclusive and filter on `timestamp_utc` (the event's
  own reported time, not `server_received_at`) — that's what a caller
  means by "everything between these two timestamps".
- `limit` still applies even when a range is given, so this stays
  "bounded history" (docs/PLAN.md) rather than letting a wide range
  return an unbounded result.
- A naive (no offset) `from`/`to` is treated as UTC — the same rule
  `telemetry_contract.models` already applies to a naive
  `timestamp_utc` on ingest, now reused via a small `_normalize_utc`
  helper in `query.py` so there's one definition of what a bare
  timestamp means, everywhere in this API, not two.
- `from` after `to` is a 422 (`invalid_request`), the same shape every
  other bad query parameter already produces — a cross-field check
  FastAPI's own per-parameter validation can't express on its own.
- The resolved `from`/`to` are echoed back in the response body, so a
  caller can see exactly what range was actually applied (including
  after UTC-normalizing a naive value).
- SQL stays a single parameterized query for every combination — no
  filter, from-only, to-only, or both — via `($2::timestamptz IS NULL
  OR timestamp_utc >= $2)` style clauses, rather than branching to
  build different SQL strings per case.

Added 7 integration tests (inclusive range, from-only, to-only, range
still bounded by limit, naive-treated-as-UTC, from-after-to is 422,
from/to echoed back), plus updated one existing regression test whose
exact-dict assertion needed the new `from`/`to` keys.

Found and fixed a real bug in the new tests themselves while writing
them, not in the product: the first version hand-built the request URL
with an f-string embedding `.isoformat()` datetimes directly, e.g.
`f"...?from={from_}&to={to}"`. A UTC offset like `+00:00` contains a
literal `+`, which a query string is not required to percent-encode
but which standard `application/x-www-form-urlencoded` decoding reads
back as a space — silently corrupting the timestamp before it ever
reached the parser. 5 of the 7 new tests failed with unexpected 422s
as a result, including `test_history_from_after_to_is_422`, which had
been "passing" for the wrong reason (any 422, not the intended
cross-field check). Confirmed the root cause with
`urllib.parse.urlencode({'from': '2026-01-01T00:01:00+00:00'})` →
`from=2026-01-01T00%3A01%3A00%2B00%3A00`. Fixed by rewriting every new
test to pass `params={"from": ..., "to": ...}` as a dict to httpx
instead of a hand-built URL, letting httpx do the percent-encoding —
the same lesson as always with query strings and special characters,
just caught here in test code instead of product code.

Verified live end-to-end, not just with pytest: started a real api +
worker + Redis + Postgres locally (disposable dev instances, not the
test ones), posted 5 real telemetry events one minute apart through
`POST /api/v1/telemetry`, let the real worker drain them into Postgres,
then hit the real `/history` endpoint over HTTP with `from` only,
`to` only, both, a naive (no-offset) `from`/`to`, `from` after `to`,
and a range combined with `limit=2` — every case matched the intended
inclusive-boundary, UTC-normalizing, still-bounded-by-limit behavior.
Shut down and cleaned up all the disposable dev processes afterward.

## 2026-09-23 — Acceptance-check verification, and a real gap found and fixed

Asked to check three acceptance-check items against the actual product:
(1) one command brings the system up, a separate command starts the
simulators; (2) all three simulated assets run for at least ten minutes
with latest state and history staying correct; (3) duplicate,
out-of-order, malformed, and silent-asset cases behave exactly as
documented and are covered by tests.

(2) and (3) were verified live, not just by reading code. For (2): ran
api + worker + all three simulators (drone-01, rover-01, quad-01) for
~12 minutes against disposable Postgres/Redis, with the same fault
rates `docker compose up` uses by default. Snapshotted `/api/v1/assets`
every 20s (36 snapshots) — `latest_seq` climbed monotonically for every
asset, zero regressions. At the end, `asset_state.latest_seq` matched
the true max stored `seq` in Postgres exactly for all three assets,
`/history` was correctly newest-first, and there wasn't a single 500 or
unhandled exception across the run (worker outcomes: 1,706
`stored_and_advanced`, 212 `duplicate_ignored`, 158 `stored_but_stale`).
For (3): cross-checked `docs/PLAN.md`'s failure-policy table against
that same live run's actual outcomes (matched exactly) and against the
existing test suite (each of the four cases already has dedicated
tests: `test_duplicate_seq_is_not_inserted_twice` and 2 more,
`test_out_of_order_event_is_stored_but_does_not_regress_state` and 2
more, 10+ malformed/rejection tests across the contract, ingest, and
simulator-corruption layers, `test_list_assets_returns_all_known_assets_with_online_status`
for silent/offline). No gaps found in either.

(1) was a real gap, not just a documentation shortfall. `docker-compose.yml`
had only ever exposed one command, `docker compose up --build`, which
started postgres, redis, api, worker, *and* the simulator together —
even though `docs/PLAN.md`'s own problem framing has said since the
very first commit that the simulator is "separate test equipment, not
part of the product." That line was never wrong, but it was never
enforced: the Docker Compose step (see that entry above) explicitly
chose to bundle the simulator into the one-command `up` so a plain
`docker compose up` would "demonstrate every failure path... without
any extra configuration" — a deliberate demo-convenience decision that,
in hindsight, traded away the very separation the plan had already
called for. Nothing caught this at the time because nothing was
checking for it: the Docker Compose step's own acceptance bar was "does
`docker compose up --build` bring up a working system with visible
traffic", which it did, honestly — the bar just didn't ask whether
*only* the product should come up.

Why it matters beyond just matching a spec: bundling test-equipment
startup into the product's own startup path means anyone bringing up
"the system" always also gets a fleet of fake assets flooding it with
traffic, whether they wanted that or not. That makes it harder to
inspect the real system in isolation (e.g. sending a few hand-crafted
events via curl to check a specific case, without random simulator
traffic interleaved), and it quietly widens what counts as "the
product" to include the thing `docs/PLAN.md` explicitly says isn't
part of it.

Fixed by giving the `simulator` service in `docker-compose.yml` a
compose profile (`profiles: ["simulator"]`). A profiled service is
excluded from every command that has to guess which services to act on
(`up`, `down`, `build` with no service names given) unless that profile
is explicitly activated with `--profile`; naming a service explicitly
(`logs`, `stop`, `down <service>`) still works either way. So now:

- `docker compose up --build` brings up only postgres, redis, api,
  worker — the real product, with zero synthetic traffic.
- `docker compose --profile simulator up --build -d simulator` is the
  separate, second command that starts the simulated fleet against the
  already-running system.
- `docker compose --profile simulator down -v` tears down everything,
  simulator included — a bare `docker compose down` only touches the
  un-profiled services and leaves the simulator container running,
  which was surprising enough on first test that it's called out
  explicitly in README.md now.

Verified for real, not just via `docker compose config`: started the
Docker daemon in this sandbox (worked this time — same transient
restriction noted in the Docker Compose step's entry, not a permanent
one) and, using that step's documented pip/proxy workaround, built and
tagged all three images to the exact names compose expects. Ran the
actual two-command sequence: `docker compose up -d` brought up exactly
four containers, api healthy, `/api/v1/assets` returning `{"assets":
[]}`; `docker compose --profile simulator up -d simulator` started a
fifth container and real telemetry began arriving seconds later. Also
tested the teardown edge case by hand before writing it into the
README: a bare `docker compose down -v` left the simulator container
running (and failed to remove the network because of it), while
`docker compose --profile simulator down -v` removed everything
cleanly — confirmed both ways rather than assuming which one was
correct. Deleted the temporary images and the copied CA bundle
afterward.

Updated to match: `docker-compose.yml`'s top comment and the
simulator service's own comment; `services/simulator/Dockerfile`'s
comment (previously said it was "included in compose so 'one command'
also gives a live... telemetry stream" — no longer true, rewritten);
`README.md`'s "Run it" section (now two commands, plus the
`down`-needs-`--profile`-too note); `.env.example`'s comment about
which command shows the fault-injection defaults; and
`.github/workflows/ci.yml`'s `docker-smoke` job, which previously
brought everything up in one `docker compose up -d --build` and would
otherwise have silently stopped proving anything once the simulator
stopped starting alongside it — it now runs both commands, asserts
`/api/v1/assets` is empty *before* the simulator starts (proving the
split actually holds, not just that traffic eventually shows up) and
non-empty after, and tears down with `--profile simulator`.
No product code changed — this was entirely about the gap between what
`docs/PLAN.md` said the architecture was and what the packaging around
it actually enforced.

## 2026-09-24 — Five improvements beyond the baseline

After RETROSPECTIVE.md and the acceptance-check verification above, asked
to actually build all five planned "beyond the baseline" directions
(fault injection, observability, poison-message handling, performance
proof, live state stream) rather than just plan how to demonstrate two
of them.
Full risk/evidence writeup with real numbers from live runs is in the
new `docs/IMPROVEMENTS.md`; this entry is the build-log version —
decisions made and what broke along the way.

**Poison-message handling.** Added a second Redis stream,
`telemetry.deadletter`, and `GET`/`POST .../deadletter/{id}/replay` on
the api. Decision made early: a plain stream, not a consumer group or a
Postgres table — a dead-letter table would mean the worker's write path
now depends on two storage systems instead of one, for a feature whose
whole point is "something about this message can't be trusted yet."
Straightforward to build since it reuses Redis Streams primitives
(`XADD`/`XRANGE`/`XDEL`) the worker and api already use elsewhere for
`telemetry.raw` — no new client library, no new connection. Verified
with `pytest` (11 new/extended tests) and then live against the running
stack: hand-crafted a bad `XADD`, confirmed it showed up in
`GET .../deadletter` with the real parse-error reason, replayed it, and
watched it get dead-lettered *again* (still literally "not valid
json", replay doesn't fix content, it just gives it another chance) —
kept that honest outcome in `docs/IMPROVEMENTS.md` rather than replaying
something pre-fixed to make the demo look cleaner.

**Real observability.** Added `prometheus_client` metrics to both
services — four total, each picked to answer a specific question
`docs/PLAN.md`'s own failure table implicitly promises an answer to
(is the backlog draining, is Postgres actually flaky, etc.), not a
generic dashboard-filler set. One real bug found while wiring this up:
the worker has no HTTP framework, so `prometheus_client.start_http_server`
runs its own tiny server on a fixed port (9100) at startup — and two
existing tests (`test_worker_redis_unavailable_recovery.py`,
`test_worker_consumer_group_recovery.py`) both call `worker.main.run()`
directly as a real background task, so running the full suite hit
`OSError: [Errno 98] Address already in use` on the second call in the
same pytest process. Fixed by wrapping the `start_http_server` call in
`try/except OSError`, logged as a warning and never fatal — which is
also just the right production behavior independent of the test
collision: losing the metrics server must never stop the worker doing
its actual job. Verified with 6 new integration tests (asserting metric
*deltas*, since `prometheus_client`'s registry is process-global and
shared across the whole test run — asserting an absolute value would
have been wrong the moment a second test file touched the same
counter) and then live: `curl localhost:8000/metrics` and
`curl localhost:9100/metrics` against the running stack, both serving
real Prometheus text output.

**Automated fault injection.** `scripts/fault_injection_demo.sh`
SIGKILLs the real `postgres` container in the live `docker compose`
stack and checks recovery against real HTTP responses and a real row
count, not a mocked failure. One design correction made while writing
it: the first draft used a fixed `sleep 1` after the kill before
sending "during outage" traffic, assuming that was long enough — but
`docker-compose.yml` already sets `restart: unless-stopped` on
`postgres`, so Docker restarts it on its own, and a fixed sleep would
have been racing against however fast that happened to be on a given
run. Replaced the sleep with an actual wait-loop on `/health/ready`
reporting unhealthy, so the script *proves* it caught the outage window
instead of assuming it did. Ran it live: 16 events sent (8 before the
kill, 8 during), zero lost, zero duplicated, worker logs showing
exactly one `db_retry` before recovery (Postgres came back fast enough
that only one message actually hit a failed attempt) — see
`docs/IMPROVEMENTS.md` for the full log excerpt and an honest note
about what a single-retry run does and doesn't prove.

**Performance proof.** `scripts/load_test.py` sends N events at a
configurable concurrency and measures ingest latency/throughput
separately from how long the worker takes to fully drain that batch —
deliberately two different measurements, not one, so a real bottleneck
could be identified instead of assumed. Assumption going in was that
the worker (one Postgres transaction per message, no internal
concurrency in its read loop) would be the slower side. The actual
measured result was the opposite: the worker drained 500 messages in
under a second (1395/s at one concurrency level) while ingest itself
was the limiting factor (245–357 req/s, latency getting *worse* going
from 50 to 100 concurrent requests) — consistent with a single
`uvicorn` process contending for this sandbox's 2 CPU cores, confirmed
by sampling `docker stats` mid-run rather than just asserting it. Kept
the wrong initial assumption and the real finding both in
`docs/IMPROVEMENTS.md`, since "the bottleneck wasn't where I expected"
is a more honest performance proof than one written to confirm a guess.

**Live state stream.** `GET /api/v1/stream` (SSE), fed by the worker
publishing to a Redis Pub/Sub channel right after its existing `ack()`
call. Pub/Sub over a Stream was a deliberate choice, not the first
thing tried by default: a Stream would need trimming or it grows
forever, while Pub/Sub simply drops a publish with zero subscribers,
so a slow or absent SSE client creates zero memory pressure on the
worker or Redis — the property this feature most needed, given the
explicit instruction not to weaken the reliability work already done.
One real tooling problem hit while writing the integration test: httpx's
`ASGITransport` (used for every other router's tests in this suite)
turned out to buffer a `StreamingResponse`'s entire body before handing
any of it back to the client — fine for finite responses, but it means
a test against an endpoint that's designed to stream until the client
disconnects just hangs forever, which is exactly what happened first
(`pytest` timeout after 17s across all three new tests). Confirmed the
cause with an isolated repro (a bare FastAPI app, printed progress
inside the generator) before concluding it was a transport limitation
and not a bug in the router — the repro showed the *entire* server-side
generator running to completion before the client's `stream()` call
even returned, which only makes sense if the transport isn't handing
data back incrementally. Rewrote the tests to drive the router's
generator function directly instead (still against real Redis Pub/Sub
and the real worker publish call site — only the HTTP layer itself is
skipped); documented why in that test file's module docstring so the
next person doesn't hit the same 17-second hang wondering what's wrong.
Verified live too: `curl -N` against the running stack, `POST`ed one
event, watched it arrive on the SSE connection with the correct
`outcome`.

**What didn't change:** none of the five additions touch the
store-before-ack rule `docs/PLAN.md` and `worker/processing.py`'s own
module docstring both call the one rule that module exists to enforce.
Each one was checked against that rule specifically before being
considered done — dead-letter write happens before the ack it replaces;
metrics recording happens after the ack (or after the dead-letter path,
for that case) and can't fail it; the state-update publish happens
after ack and swallows its own errors; the fault-injection script and
load test only ever call the public API, never touch worker internals.

Full test suite after all five: 123 passing (108 baseline + 6
poison-message + 6 metrics + 3 stream — the fault-injection and
load-test evidence lives in `docs/IMPROVEMENTS.md` instead of the test
suite, since those two are live demonstrations against a running stack
rather than repeatable pytest cases).
Updated to match: `README.md` (API reference table, a new
"Demonstrating the improvements" section, `docs/IMPROVEMENTS.md` and
`docs/RETROSPECTIVE.md` linked from the top, repository layout, test
count); `docker-compose.yml` (worker's `9100` metrics port exposed);
both `services/api/pyproject.toml` and `services/worker/pyproject.toml`
(`prometheus-client` added as a real declared dependency, not just
installed ad hoc in the dev venv).

## 2026-09-27 — 2026-09-28 — Critical self-review: 13 findings, all fixed

Asked for a fresh, adversarial pass over the whole repo before tagging
`v1.0.0` — not a re-read of `docs/PLAN.md`'s own claims, but trying
deliberately to break the system's stated guarantees. It found 13 real
issues (3 critical, 5 significant, 5 minor), none previously caught by
the test suite or the acceptance-check verification above. Fixed all
13, each with its own commit, its own regression test where the code
path was testable by pytest, and re-verified against real Postgres +
Redis before moving to the next one — never batched untested.

**Critical (all three were "the system lies about its own
guarantees" bugs, not edge-case polish):**
- A NUL byte in `asset_id`, or NaN/Infinity hidden in an `extra=allow`
  field, passed contract validation untouched and then crashed the
  worker permanently at the Postgres write — the same bytes fail the
  same way on every retry, so one bad event could wedge the queue
  forever. Fixed by rejecting both at the contract layer (recursively,
  including nested objects/lists), plus a worker-side backstop in case
  something reaches `store_event` anyway.
- Dead-letter replay's lookup/re-enqueue/delete were three separate
  Redis round-trips with nothing tying them together — a Redis blip
  between them, or two replay requests for the same id at once, could
  duplicate or silently drop a message. Fixed by making all three one
  atomic Lua script.
- A malformed replay id raised a Redis `ResponseError` (a `RedisError`
  subclass), which the router reported as `503 queue_unavailable` —
  indistinguishable from a real outage to anything watching that status
  code. Fixed by catching that specific case and returning `400`.

**Significant:**
- No body-size limit on `POST /api/v1/telemetry` — a single oversized
  or deeply nested payload was fully buffered and parsed before
  validation ever ran. Fixed with ASGI middleware enforcing a
  configurable byte cap, checked against `Content-Length` and against
  actual bytes read (so a chunked body without `Content-Length` can't
  bypass it), before FastAPI's own parsing starts.
- Two worker replicas could both claim and process the same message —
  `XAUTOCLAIM`'s idle threshold doesn't know a message is still being
  retried by a live worker. Fixed with a per-message Redis lock
  (acquire/extend/release, all exclusive to the current holder).
- Concurrent replay of the same dead-letter id wasn't locked — closed
  by the same atomic Lua script as the critical dead-letter fix above.
- `plan_tick`'s `should_corrupt()` short-circuited `should_reorder()`,
  so the out-of-order rate was quietly lower than `SIM_OUT_OF_ORDER_RATE`
  says whenever bad-data injection also fired on the same tick. Fixed
  by computing both independently. Verified with a statistical
  regression test (4,000 simulated ticks, both rates non-zero) checking
  the observed reorder rate lands within tolerance of the configured
  one — a plain before/after read wouldn't have caught a rate drifting
  by degree rather than breaking outright.
- `scripts/fault_injection_demo.sh` (the one real Postgres
  crash-recovery proof) was never called by CI — only pytest's mocked
  recovery tests ran automatically. Added a dedicated `fault-injection`
  CI job that runs it against a real `docker compose up` stack on every
  push.

**Minor:**
- Dead-letter replay never touched `blackbox_ingest_events_total`, so
  replayed volume understated real traffic reaching `telemetry.raw`.
  Fixed with a new `outcome="replayed"` label.
- The SSE stream's asset_id filter only caught `json.JSONDecodeError`;
  valid JSON that isn't an object (a bare number, string, list, or
  null) raised an uncaught `AttributeError` instead, which would have
  killed the generator for every filtered subscriber. Unreachable
  today (the worker only ever publishes objects) but fixed anyway —
  wrong input from *anything* on that channel should be skipped, not
  crash the reader.
- Postgres credentials were three separate hardcoded strings in
  `docker-compose.yml` with nothing tying them together — missing one
  during a credential change failed at runtime, not at parse time.
  Fixed with a single `${POSTGRES_*:-default}` override point read
  everywhere a credential is needed.
- `scripts/load_test.py` used a bare `asyncio.gather` — the first
  failure propagated immediately without cancelling sibling tasks, so
  the `async with client:` block closed the client out from under
  requests still in flight. Fixed with explicit tasks +
  `asyncio.wait(..., return_when=FIRST_EXCEPTION)` + a `finally` block
  that cancels and awaits every task before the client is allowed to
  close. Verified with an instrumented mock tracking whether any
  request was still in-flight at the moment the client closed: 19 of
  20 concurrent sends were abandoned mid-request with the old code,
  zero with the fix.
- Two of `fault_injection_demo.sh`'s three `wait_for` calls relied on
  `set -e` instead of the script's own `FAILURES` tally, so a slow
  Postgres restart could abort the script before the one check it
  exists to make (step 6, zero data loss) ever ran. Fixed with
  `|| true` on both (the timeout is still recorded via `fail()`
  either way — only the early exit is avoided).

**A follow-up caught while closing out the last minor finding:** the
`fault-injection` CI job (added above) ran the script as
`./scripts/fault_injection_demo.sh`, relying on its executable bit.
GitHub's Contents API — used by the file-push tooling this build
relies on — always writes blobs at mode `100644` and has no way to
request `100755`, so a push could silently land the script
non-executable on GitHub with the local working tree showing no
difference at all. No tool available in this environment exposes a
pushed file's git blob mode, so this couldn't be directly confirmed
either way. Rather than leave it as an unverified assumption, changed
the CI step to `run: bash scripts/fault_injection_demo.sh`, which
doesn't depend on the file's mode bit regardless of how it landed.

Full suite after all 13 fixes: 160 tests passing (up from 123),
re-run against real Postgres + Redis, not from a stale commit-message
count. `README.md`'s test count and command were the only
documentation actually out of date after this round; updated in the
same pass as this entry.

## 2026-10-08 — Portfolio polish, round 1: de-internalize + 60-second reviewability

The task is closed (mentor review returned: EXCEEDS, 9.4/10). Owner is
making the repo public as a portfolio piece and asked for this to
continue as an open-ended project rather than more task work, staged
explicitly: de-internalize the docs, then make the repo reviewable in
60 seconds, stop for review, then (a separate, not-yet-started round)
engineering hygiene and production-minded additions. This entry covers
the first two stages only. Work happened on a new `portfolio-polish`
branch; `main` untouched throughout.

**Baseline before touching anything:** full suite run against real
Postgres/Redis — 160 passed, 0 failed, 0 errors. (Docker's daemon
wasn't reachable in this sandbox session; native `postgresql`/
`redis-server` services stood in, matching how earlier build steps in
this same log handled the same constraint.)

**De-internalize.** Grepped `README.md` and every `docs/*.md` file for
task-specific framing — "the brief," "submission," "adversarial
review," task numbering — and reworded each instance without touching
substance. First pass caught 10 instances across `README.md`,
`docs/PLAN.md`, and `docs/RETROSPECTIVE.md`; a second, broader sweep
(word-boundary regex had missed hyphenated and adjacent forms) caught
3 more: `README.md`'s "the adversarial-review fix round" →
"the critical self-review fix round," `docs/PLAN.md`'s "AI/ML/agent
logic (excluded by the task)" → "(out of scope for this project)," and
this file's own "brief says it should be short" → "to stay short and
skimmable." `docs/PROCESS_LOG.md` and `docs/RETROSPECTIVE.md` were
explicitly kept honest and intact otherwise — the bug lists, the real
mistakes, the "what my checks missed" section all stayed exactly as
written, only the framing words changed. Final grep across all 5 doc
files for `brief|submission|adversarial review|task 0?1|mentor|
grading|star bank`: zero matches.

**Fresh numbers, not reused ones.** Re-ran both proof scripts rather
than trusting any number already written down. Docker wasn't available
this session either, so `scripts/load_test.py` (which assumes `docker
compose up --build -d`) and `scripts/fault_injection_demo.sh` (which
assumes `docker compose kill/start postgres`) both needed a substitute
runtime, same workaround pattern as earlier in this build:

- api and worker ran as plain `uvicorn app.main:app` /
  `python -m worker.main` processes against the real `blackbox`
  Postgres database and a real Redis instance on `localhost:6379` —
  same code, same schema, no mocks, just no container boundary.
- `scripts/load_test.py --count 500 --concurrency 50` run as-is,
  unmodified, against that stack: 208.2 req/s ingest, p50/p95/p99 =
  167.1/559.1/800.0ms, worker drain 745.7 events/s (worker keeps up
  with ingest at this load — matches the script's own framing that
  it's measuring, not assuming, which side is the bottleneck).
- `scripts/fault_injection_demo.sh` itself was **not** run as-is — it
  shells out to `docker compose kill/start postgres` directly, which
  doesn't exist without Docker. Rather than editing the committed
  script to special-case a sandbox limitation (the Docker-based
  version is correct for its documented, normal environment), ran a
  one-off local copy that substitutes `service postgresql
  stop`/`start` for the two docker calls and keeps every other line —
  same before/during/after structure, same exact-count pass/fail
  check, same asset-id-per-run isolation. Not committed to the repo.
  Result: 8 events before the outage + 8 during it, all 16 stored,
  zero lost, zero duplicated; outage-to-`/health/ready` window 2.58s;
  worker log confirms the one in-flight message retried exactly once
  at the documented 0.5s initial backoff delay before Postgres came
  back. This is the same claim the committed script proves
  (`docs/PLAN.md` §4: "Postgres unavailable → message waits, never
  dropped"), proven the same way, just without the container.
  Worker-kill recovery (as opposed to Postgres-kill) wasn't separately
  re-timed this round — it's proven correct by
  `tests/integration/test_worker_restart_recovery.py` and bounded by
  `CLAIM_MIN_IDLE_MS = 30s` in code, and the README says so plainly
  rather than implying a live-timed number that wasn't actually taken.

**60-second reviewability.** Rewrote the top of `README.md`: a CI
badge, a one-paragraph problem statement (no architecture detail, just
what breaks without this system and what it guarantees), a Mermaid
architecture diagram (rendered and visually checked locally with
`mmdc` before committing — this sandbox has no Chrome for `mmdc` by
default, resolved by pointing `PUPPETEER_EXECUTABLE_PATH` at the
Playwright-installed Chromium already present), the Key results table
above with its one environment caveat stated inline rather than
buried, and a Failure modes table (failure / detection / behavior /
recovery / proving test) built from the real test names in
`tests/integration/`, not paraphrased from `docs/PLAN.md`'s policy
table.

Added `docs/adr/` — four short ADRs (0001 Redis Streams over
Kafka/RabbitMQ, 0002 idempotent inserts + monotonic state, 0003
at-least-once delivery, 0004 known limits and 100x scale) expanding on
`docs/PLAN.md`'s existing D1/D2/D3/D5 decisions with the fuller
argument, explicit tradeoffs, and — for 0004 — limits that aren't
written down anywhere else yet (fixed small Postgres pool sizes, no
rate limiting, single-node Redis/Postgres, one worker with no internal
concurrency). README's new "Design decisions" section links each ADR
with a one-line summary.

Full suite re-run after all doc/README/ADR changes (no product code
touched this round): 160 passed, 0 failed, 0 errors — unchanged from
the baseline above, as expected for a docs-only round, but re-verified
rather than assumed.

**Skipped this round, on purpose:** everything in stages 3-5 of the
owner's instructions (ruff/mypy/pre-commit/coverage
badge/Dependabot/LICENSE/.env secret audit/non-root Docker
users/dependency pinning; API-key auth + rate limiting; retention
policy; schema migrations; OpenTelemetry; `scripts/demo.sh` +
terminal recording) — explicitly deferred per the owner's own staged
instructions ("work in stages and stop for review after step 2"), not
started.
