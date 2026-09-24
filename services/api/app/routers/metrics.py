"""GET /metrics — Prometheus text exposition.

Deliberately at the bare path, not under /api/v1: this is operational
surface for a scraper, not part of the product's data API — same
reasoning /health/* already uses (see health.py's module docstring).
"""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

router = APIRouter(tags=["metrics"])


@router.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
