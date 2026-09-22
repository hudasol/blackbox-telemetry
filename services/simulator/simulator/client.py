"""Thin HTTP layer: POSTs one event to the ingest API and logs the
outcome.

There's deliberately nothing special here about a duplicate, reordered,
or corrupted send — from the wire's point of view they're all just a
POST with some payload. That's the point: the switches change what gets
built and when it's sent (simulator/scheduler.py, simulator/faults.py),
not how it's sent.
"""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger("simulator.client")


async def send_event(client: httpx.AsyncClient, base_url: str, event: dict) -> int:
    """POST one event; returns the HTTP status code.

    Never raises for an ordinary HTTP error response (4xx/5xx) — those
    are expected outcomes for a corrupted send, not simulator bugs. A
    connection-level failure (the API is down) does raise, since the
    caller needs to tell the difference (docs/PLAN.md's "ingest
    unavailable" isn't this service's failure to hide).
    """
    response = await client.post(f"{base_url}/api/v1/telemetry", json=event)
    logger.info(
        "simulator.sent",
        extra={
            "event": "simulator.sent",
            "asset_id": event.get("asset_id"),
            "seq": event.get("seq"),
            "status_code": response.status_code,
            "simulated_fault": event.get("_simulated_fault"),
        },
    )
    return response.status_code
