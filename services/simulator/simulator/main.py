"""Simulator entrypoint: runs the fake fleet against a real ingest API.

Test equipment, not part of the product (docs/PLAN.md, problem framing)
— its only job is producing a plausible, occasionally-broken telemetry
stream so the rest of the system can be exercised end-to-end, including
the failure paths. Each asset runs its own independent tick loop; the
fault-injection switches are per-run config (see simulator/config.py).
"""

from __future__ import annotations

import asyncio
import logging
import random
import signal

import httpx

from .assets import SimulatedAsset
from .client import send_event
from .config import get_settings
from .faults import FaultInjector, FaultRates
from .logging_config import configure_logging
from .scheduler import TickPlan, plan_tick

logger = logging.getLogger("simulator")


async def send_plan(
    client: httpx.AsyncClient, base_url: str, plan: TickPlan, injector: FaultInjector
) -> list[int]:
    """Send every event in a scheduling decision, applying the duplicate
    switch per event as it goes out. Returns each HTTP status code, in
    send order.

    Kept separate from the run loop below so it's directly reusable —
    integration tests drive plan_tick + send_plan by hand instead of
    waiting on the loop's real sleep (tests/integration/test_simulator_faults_e2e.py).
    """
    statuses: list[int] = []
    for payload in plan.to_send:
        statuses.append(await send_event(client, base_url, payload))
        if injector.should_duplicate():
            statuses.append(await send_event(client, base_url, payload))
    return statuses


async def _run_asset(
    asset: SimulatedAsset,
    injector: FaultInjector,
    client: httpx.AsyncClient,
    base_url: str,
    tick_interval: float,
    force_silent: bool,
    shutdown_event: asyncio.Event,
) -> None:
    held: dict | None = None
    while not shutdown_event.is_set():
        plan = plan_tick(asset, injector, held, force_silent=force_silent)
        held = plan.held
        try:
            await send_plan(client, base_url, plan, injector)
        except httpx.HTTPError as exc:
            # The API being unreachable is a real, expected condition
            # (docs/PLAN.md's own "Redis unavailable" -> 503 path can
            # cause this) — log and keep ticking rather than crash the
            # whole simulator over one asset's bad luck.
            logger.warning(
                "simulator.send_failed",
                extra={
                    "event": "simulator.send_failed",
                    "asset_id": asset.asset_id,
                    "reason": str(exc),
                },
            )

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=tick_interval)
        except asyncio.TimeoutError:
            pass


async def run(shutdown_event: asyncio.Event) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    rng_seed = (
        settings.seed
        if settings.seed is not None
        else random.SystemRandom().randrange(2**31)
    )
    logger.info(
        "simulator.startup",
        extra={
            "event": "simulator.startup",
            "api_base_url": settings.api_base_url,
            "fleet_size": len(settings.fleet),
            "seed": rng_seed,
            "duplicate_rate": settings.duplicate_rate,
            "out_of_order_rate": settings.out_of_order_rate,
            "bad_data_rate": settings.bad_data_rate,
            "silence_rate": settings.silence_rate,
            "force_silent_asset_ids": list(settings.force_silent_asset_ids),
        },
    )

    rates = FaultRates(
        duplicate=settings.duplicate_rate,
        out_of_order=settings.out_of_order_rate,
        bad_data=settings.bad_data_rate,
        silence=settings.silence_rate,
    )
    # A single top-level Random derives one child seed per asset (state
    # + fault draws each), so the whole run is reproducible from one
    # SIM_SEED without assets racing each other over a shared Random.
    fleet_rng = random.Random(rng_seed)

    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds) as client:
        tasks = []
        for spec in settings.fleet:
            asset_rng = random.Random(fleet_rng.random())
            injector_rng = random.Random(fleet_rng.random())
            asset = SimulatedAsset(
                asset_id=spec.asset_id, asset_type=spec.asset_type, rng=asset_rng
            )
            injector = FaultInjector(rates, injector_rng)
            force_silent = spec.asset_id in settings.force_silent_asset_ids
            tasks.append(
                asyncio.create_task(
                    _run_asset(
                        asset,
                        injector,
                        client,
                        settings.api_base_url,
                        settings.tick_interval_seconds,
                        force_silent,
                        shutdown_event,
                    )
                )
            )

        await shutdown_event.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("simulator.shutdown", extra={"event": "simulator.shutdown"})


def _install_signal_handlers(shutdown_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)


async def main() -> None:
    shutdown_event = asyncio.Event()
    _install_signal_handlers(shutdown_event)
    await run(shutdown_event)


if __name__ == "__main__":
    asyncio.run(main())
