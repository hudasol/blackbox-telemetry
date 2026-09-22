# Built with the repo root as context (see docker-compose.yml) so this
# can reach libs/telemetry_contract — the workspace-local shared package
# every service installs separately, same as local dev (docs/PLAN.md).
FROM python:3.12-slim

WORKDIR /app

COPY libs/telemetry_contract /app/libs/telemetry_contract
RUN pip install --no-cache-dir /app/libs/telemetry_contract

COPY services/worker/pyproject.toml /app/services/worker/pyproject.toml
COPY services/worker/worker /app/services/worker/worker
RUN pip install --no-cache-dir /app/services/worker

WORKDIR /app/services/worker

# No HTTP surface to healthcheck (nothing in compose depends_on this
# service) — its own bounded-backoff/self-heal behavior (worker/main.py)
# is what keeps it alive, not an orchestrator-level check.
CMD ["python", "-m", "worker.main"]
