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
