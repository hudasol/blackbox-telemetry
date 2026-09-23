# Built with the repo root as context (see docker-compose.yml) so this
# can reach libs/telemetry_contract — the workspace-local shared package
# every service installs separately, same as local dev (docs/PLAN.md).
#
# Test equipment, not part of the product (docs/PLAN.md, problem
# framing) — kept behind the "simulator" compose profile (see
# docker-compose.yml) so bringing up the real system never implies
# bringing up synthetic traffic too; started with its own explicit
# second command instead.
FROM python:3.12-slim

WORKDIR /app

COPY libs/telemetry_contract /app/libs/telemetry_contract
RUN pip install --no-cache-dir /app/libs/telemetry_contract

COPY services/simulator/pyproject.toml /app/services/simulator/pyproject.toml
COPY services/simulator/simulator /app/services/simulator/simulator
RUN pip install --no-cache-dir /app/services/simulator

WORKDIR /app/services/simulator
CMD ["python", "-m", "simulator.main"]
