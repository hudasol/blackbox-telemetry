# blackbox — fleet telemetry backend

A resilient, reproducible telemetry service for a simulated inspection fleet
(drone, ground rover, quadruped). No AI/ML/agent logic inside the product —
this is the systems layer: ingest → queue → worker → persistence → query,
built to fail predictably instead of quietly.

Full design, data contract, and failure policy: [`docs/PLAN.md`](docs/PLAN.md).
Build history, decisions, and every bug found along the way (and how it was
found): [`docs/PROCESS_LOG.md`](docs/PROCESS_LOG.md). Five additions beyond
the baseline — poison-message handling, real observability, automated fault
injection, a measured performance proof, and a live state stream — each
tied to a specific engineering risk and backed by a real run, not a
claim: [`docs/IMPROVEMENTS.md`](docs/IMPROVEMENTS.md). Honest retrospective
— what went wrong, bad assumptions, and what pre-submission checks
missed: [`docs/RETROSPECTIVE.md`](docs/RETROSPECTIVE.md).

```
   simulators (test equipment, not part of the product)
   drone-sim   rover-sim   quadruped-sim
            |  HTTP POST /api/v1/telemetry
            v
   api (FastAPI) — ingest + query + health
            |  XADD                     ^  SELECT
            v                            |
   redis stream: telemetry.raw
            |  XREADGROUP / XACK / XAUTOCLAIM
            v
   worker — idempotent, monotonic-state insert
            |  INSERT / UPSERT
            v
   postgresql — telemetry_events (raw) + asset_state (latest snapshot)
```

## Run it (two commands)

Requires Docker with Compose v2 (`docker compose`, not `docker-compose`).

```bash
# 1. Bring up the real system: Postgres, Redis, the api (ingest + query),
#    and the worker. No synthetic traffic yet.
docker compose up --build -d
```

This builds and starts the actual product — wired together on one
network, with no manual setup step: the worker bootstraps the Postgres
schema itself at startup. Services come up in real dependency order
(`depends_on: condition: service_healthy`, not a fixed sleep). The api
is reachable at `http://localhost:8000` once it's up; `GET
/api/v1/assets` returns `{"assets": []}` until something sends telemetry
— there's no fleet running yet on purpose.

```bash
# 2. Start the simulated fleet — separate test equipment (docs/PLAN.md,
#    problem framing), not part of the product, so it's a second,
#    explicit command against the system that's already up.
docker compose --profile simulator up --build -d simulator
```

This starts a small default fleet (`drone-01`, `rover-01`, `quad-01`)
sending real HTTP traffic to the api that's already running, with fault
injection **on by default** (duplicates, out-of-order sequences,
rejected payloads, one asset occasionally going silent) so this second
command alone demonstrates every failure path in `docs/PLAN.md` without
any extra configuration.

Stop everything with `docker compose --profile simulator down -v` (`-v`
also drops the Postgres volume, for a clean start next time). The
`--profile simulator` flag matters here: a bare `docker compose down`
only acts on the un-profiled services (same set a bare `docker compose
up` starts) and leaves the simulator container running. To stop just
the fleet and keep the system running, `docker compose stop simulator`
works fine without the flag — profiles only gate a command that has to
guess *which* services to act on; naming one explicitly (`logs`,
`stop`, `down <service>`) always works.

### Configuring the demo

Copy `.env.example` to `.env` to override any default — e.g. set every
`SIM_*_RATE` to `0` for a clean, uneventful fleet, or lower
`ASSET_OFFLINE_TIMEOUT_SECONDS` to see an asset flip offline faster.
See `.env.example` for the full list and what each knob does.

## Demo: watching it work

With the system and the simulated fleet both up (the two commands
above), in another terminal:

```bash
# Structured logs from every service, interleaved
docker compose logs -f api worker simulator

# Liveness / readiness
curl -s http://localhost:8000/health/live | python3 -m json.tool
curl -s http://localhost:8000/health/ready | python3 -m json.tool

# The fleet, with computed online/offline status
curl -s http://localhost:8000/api/v1/assets | python3 -m json.tool

# One asset's current state
curl -s http://localhost:8000/api/v1/assets/drone-01/latest | python3 -m json.tool

# Its recent raw history (newest first)
curl -s http://localhost:8000/api/v1/assets/drone-01/history?limit=5 | python3 -m json.tool

# Everything between two timestamps, instead of "most recent N" — from
# and to are both inclusive, and limit still applies as an upper bound
# even when a range is given
curl -s -G http://localhost:8000/api/v1/assets/drone-01/history \
  --data-urlencode "from=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)" \
  --data-urlencode "to=$(date -u +%Y-%m-%dT%H:%M:%SZ)" | python3 -m json.tool

# Send one event by hand (timestamp_utc must be close to "now" — the api
# rejects anything more than 60s in the future, so it's generated here
# rather than hard-coded)
curl -s -X POST http://localhost:8000/api/v1/telemetry \
  -H 'Content-Type: application/json' \
  -d "{
        \"asset_id\": \"drone-01\", \"asset_type\": \"drone\", \"seq\": 999999,
        \"timestamp_utc\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
        \"position\": {\"x_m\": 1.0, \"y_m\": 2.0, \"z_m\": 3.0},
        \"speed_mps\": 4.2, \"heading_deg\": 180.0, \"battery_pct\": 87.5,
        \"temperature_c\": 25.0, \"link_quality_pct\": 92.0, \"mode\": \"moving\"
      }" | python3 -m json.tool
```

To see the failure-recovery behavior described in `docs/PLAN.md`, kill
and restart the worker mid-stream and confirm nothing is lost:

```bash
docker compose kill -s SIGKILL worker
docker compose up -d worker
# the simulator keeps sending the whole time; once the worker is back,
# GET /api/v1/assets/<id>/latest reflects everything it missed — nothing
# dropped, nothing double-applied (idempotent insert + monotonic state).
```

## Demonstrating the improvements

Five additions beyond the baseline, each with a script or endpoint that
proves it live rather than just describing it — full writeup with real
numbers from an actual run: [`docs/IMPROVEMENTS.md`](docs/IMPROVEMENTS.md).
With the system up (first command above):

```bash
# Poison-message handling: inspect and replay a message the worker
# could never parse, instead of it being silently discarded
curl -s http://localhost:8000/api/v1/deadletter | python3 -m json.tool
curl -s -X POST http://localhost:8000/api/v1/deadletter/<id>/replay

# Real observability: Prometheus metrics (ingest outcomes/latency on the
# api; processing outcomes/latency, DB retries, and stream backlog on
# the worker, which serves its own separately since it has no HTTP
# framework of its own)
curl -s http://localhost:8000/metrics
curl -s http://localhost:9100/metrics

# Automated fault injection: SIGKILLs the real postgres container mid-
# stream and proves zero data loss/duplication once it's back
./scripts/fault_injection_demo.sh

# Performance proof: measured ingest latency/throughput vs. how fast the
# worker actually drains the backlog — a measured bottleneck, not a guess
python3 scripts/load_test.py --count 500 --concurrency 50

# Live state stream: Server-Sent Events, updated the moment the worker
# stores+acks a message (optionally filtered to one asset)
curl -N "http://localhost:8000/api/v1/stream?asset_id=drone-01"
```

## API reference

All endpoints under `/api/v1`. Every response is JSON.

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/telemetry` | Accept one telemetry event. `202` + `{status, asset_id, seq, stream_id}` on success. `422` on a malformed/invalid event (never enqueued). `503` if Redis can't be reached (never a false accept). |
| `GET` | `/api/v1/assets` | List every known asset with its latest snapshot and computed `online` status. |
| `GET` | `/api/v1/assets/{asset_id}/latest` | One asset's current snapshot. `404` if the asset has never sent anything. |
| `GET` | `/api/v1/assets/{asset_id}/history?limit=N&from=...&to=...` | That asset's raw event history, newest first (`limit` 1–500, default 50). `from`/`to` (both optional, ISO 8601, inclusive) filter to everything between two timestamps; a naive value with no offset is treated as UTC. `limit` still applies as an upper bound even when a range is given. `from` after `to` is a `422`. An unknown asset is a valid empty result, not a `404`. |
| `GET` | `/api/v1/stream?asset_id=...` | Live asset-state updates over Server-Sent Events — a real-time complement to polling `.../history`, not a replacement for it. `asset_id` (optional) filters to one asset. See `docs/IMPROVEMENTS.md`, "live state stream". |
| `GET` | `/api/v1/deadletter?limit=N` | List messages the worker could never parse (newest first, `limit` 1–500, default 50) — preserved instead of discarded. See `docs/IMPROVEMENTS.md`, "poison-message handling". |
| `POST` | `/api/v1/deadletter/{id}/replay` | Put one dead-lettered message back onto `telemetry.raw` for normal reprocessing, and remove it from the dead-letter list. `404` if the id isn't there. |
| `GET` | `/health/live` | Always `200` if the process is up — no dependency checks. |
| `GET` | `/health/ready` | `200` only if Redis *and* Postgres are both reachable right now; `503` otherwise, with a per-dependency `checks` breakdown. |
| `GET` | `/metrics` | Prometheus text exposition for the api (ingest outcomes, ingest latency). The worker exposes its own separately on port `9100` (no HTTP framework of its own to add a route to). See `docs/IMPROVEMENTS.md`, "real observability". |

Field-level shapes (request and response) are defined by the shared
contract in `libs/telemetry_contract` and enforced identically on ingest;
see `docs/PLAN.md` §3 for the full field list and validation rules.

## Running without Docker (local dev)

Each service is a separate installable package sharing one contract
package, `libs/telemetry_contract`. From the repo root, with Python 3.12+:

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e libs/telemetry_contract
pip install -e "services/api[dev]"
pip install -e "services/worker[dev]"
pip install -e "services/simulator[dev]"
```

Then, each in its own terminal (against a Postgres and Redis you already
have running — e.g. `postgresql://blackbox:blackbox@localhost:5432/blackbox`
and `redis://localhost:6379/0`, or adjust `REDIS_URL`/`POSTGRES_DSN`):

```bash
cd services/api && uvicorn app.main:app --reload
cd services/worker && python -m worker.main
cd services/simulator && python -m simulator.main
```

The worker creates its own schema on first connect — no migration step.

## Tests

Unit tests exercise pure logic (the contract's validation rules, the
simulator's fault injection and scheduling, the worker's backoff timing)
with no external services. Integration tests run against **real**
Postgres and Redis — never mocks — including the failure cases from
`docs/PLAN.md` §4 (duplicate events, out-of-order/stale sequences, a
worker killed mid-stream, Postgres or Redis going unreachable). See
`docs/PROCESS_LOG.md`'s "Tests" entry for how that coverage was audited
against the brief's failure table.

Point the suite at a disposable Postgres and Redis on the ports the
tests default to (matches what CI does in `.github/workflows/ci.yml`):

```bash
docker run -d --name blackbox-test-postgres -p 5432:5432 \
  -e POSTGRES_USER=blackbox -e POSTGRES_PASSWORD=blackbox \
  -e POSTGRES_DB=blackbox_test postgres:16-alpine

docker run -d --name blackbox-test-redis -p 16379:6379 redis:7-alpine
```

Then, with the local dev install from above (`services/*[dev]` extras
bring in `pytest`, `pytest-asyncio`, `httpx`):

```bash
pytest tests/ -v
```

160 tests, all passing — re-verified against real Postgres/Redis as of
the adversarial-review fix round (see `docs/PROCESS_LOG.md`'s last
entry). If your Postgres/Redis run on different ports or credentials,
override with `POSTGRES_TEST_DSN` and `REDIS_TEST_URL` (defaults are
in `tests/integration/conftest.py`):

```bash
POSTGRES_TEST_DSN=postgresql://blackbox:blackbox@localhost:5432/blackbox_test \
REDIS_TEST_URL=redis://localhost:16379/0 \
pytest tests/ -v
```

## CI

`.github/workflows/ci.yml` runs the full test suite on every push and
pull request, against real Postgres/Redis service containers (not
mocks — same DSNs the tests default to locally), plus a fast
`docker compose config --quiet` check so a broken compose file is caught
without paying for a full multi-image build.

## Repository layout

```
libs/telemetry_contract/   shared event contract (pydantic model, constants)
services/api/               ingest (POST /telemetry) + query + health + deadletter + stream + metrics — FastAPI
services/worker/            Redis stream -> Postgres, idempotent + crash-safe
services/simulator/         fake fleet + fault injection (test equipment only)
scripts/                    fault_injection_demo.sh, load_test.py — live proof for docs/IMPROVEMENTS.md
tests/unit/                 pure-logic tests, no external services
tests/integration/          real Postgres + real Redis, incl. failure cases
docker-compose.yml          system + simulated fleet (kept as two separate commands)
docs/PLAN.md                design: architecture, contract, failure policy
docs/PROCESS_LOG.md         running build log — decisions, bugs found, fixes
docs/IMPROVEMENTS.md        five additions beyond the baseline, each with real-run evidence
docs/RETROSPECTIVE.md       honest account of mistakes, lessons, and pre-submission gaps
```
