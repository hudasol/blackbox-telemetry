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
  pass: line up `docs/PLAN.md`'s failure-policy table, the brief's own
  seven rows, against what's actually covered, and close whatever
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
