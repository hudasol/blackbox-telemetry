# Process log

Running log of framing, decisions, blockers, and changes made while
building. Newest entry at the bottom.

---

## 2026-09-22 — Repository setup + plan

- Task received: `task-01-blackbox-brief.pdf`, assigned start 2026-09-22,
  due 2026-09-29 23:59 UAE time.
- Repo scaffolded (`chore: repository setup`): directory structure only
  (`services/api`, `services/worker`, `services/simulator`,
  `libs/telemetry_contract`, `tests/`, `.github/workflows/`), no
  implementation code, per the brief's requirement that the plan is the
  first meaningful commit.
- Wrote `docs/PLAN.md` covering problem framing, architecture, the
  telemetry data contract, the failure policy table, and five engineering
  decisions (asked for at least three):
  - D1: `seq`, not `timestamp_utc`, is the ordering/idempotency authority.
  - D2: dedup enforced at Postgres, not at the ingest API, so Postgres
    outages can't take down ingestion.
  - D3: online/offline computed at query time from `last_seen_at`, not a
    stored flag flipped by a background sweep.
  - D4: ingest + query share one `api` service for now, not two services.
  - D5: Redis Streams with a consumer group, not pub/sub — pub/sub can't
    survive a down consumer, which would violate the no-silent-loss
    requirement directly.
- Blocker: `git push` to `https://github.com/hudasol/blackbox-telemetry.git`
  was rejected by the session's egress proxy — this session isn't
  authorized to push to that repo yet, so commits exist locally
  (`chore: repository setup`) pending an authorization method (repo
  connected as a session source, or a scoped personal access token).
  Resolved once GitHub was connected for this session — pushed via the
  GitHub connector instead of raw `git push`.
