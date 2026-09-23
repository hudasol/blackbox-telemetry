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
