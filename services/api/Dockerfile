# Built with the repo root as context (see docker-compose.yml) so this
# can reach libs/telemetry_contract — the workspace-local shared package
# every service installs separately, same as local dev (docs/PLAN.md).
FROM python:3.12-slim

WORKDIR /app

COPY libs/telemetry_contract /app/libs/telemetry_contract
RUN pip install --no-cache-dir /app/libs/telemetry_contract

COPY services/api/pyproject.toml /app/services/api/pyproject.toml
COPY services/api/app /app/services/api/app
RUN pip install --no-cache-dir /app/services/api

WORKDIR /app/services/api
EXPOSE 8000

# Checks /health/ready, not /health/live: this is what docker-compose's
# depends_on/service_healthy uses to hold the simulator back until the
# api can actually reach Redis and Postgres over the compose network,
# not just until the process is up (docs/PLAN.md: liveness vs readiness
# are different questions — this container's healthcheck deliberately
# asks the readiness one).
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=10 \
    CMD python -c "import urllib.request as u; u.urlopen('http://localhost:8000/health/ready', timeout=2)" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
