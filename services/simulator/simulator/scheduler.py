"""Turns one asset's tick into a decision about what to actually send.

Pure and side-effect free (no I/O, no sleeping) so the four fault
behaviors — duplicates, out-of-order, bad data, going silent — are each
independently unit-testable without a running server. See
docs/PLAN.md's failure policy for what each one is meant to exercise.
"""

from __future__ import annotations

from dataclasses import dataclass

from .assets import SimulatedAsset
from .faults import FaultInjector


@dataclass
class TickPlan:
    to_send: list[dict]
    """Events to send this tick, in send order (may be out of seq order)."""

    held: dict | None
    """An event withheld this tick, to be released — out of order — on a
    later tick. None if nothing is currently held."""


def plan_tick(
    asset: SimulatedAsset,
    injector: FaultInjector,
    held: dict | None,
    force_silent: bool = False,
) -> TickPlan:
    new_event = asset.tick()

    if force_silent or injector.should_go_silent():
        # Nothing transmitted this tick. seq already advanced above — the
        # asset's own clock kept ticking, it just didn't get through.
        # No special handling is needed on the receiving end: "online" is
        # computed from last_seen_at at read time (docs/PLAN.md, D3).
        return TickPlan(to_send=[], held=held)

    if injector.should_corrupt():
        # This tick's real data never reaches Postgres — the corrupted
        # payload should bounce with a 422 and never be enqueued.
        return TickPlan(to_send=[injector.corrupt(new_event)], held=held)

    if held is None and injector.should_reorder():
        # Hold this one back; it'll be released only after a later
        # (higher-seq) event has already gone out, so it arrives at the
        # API genuinely out of order rather than just delayed.
        return TickPlan(to_send=[], held=new_event)

    if held is not None:
        # Release the held (older-seq) event right after the current
        # (newer-seq) one — that ordering on the wire is the whole point.
        return TickPlan(to_send=[new_event, held], held=None)

    return TickPlan(to_send=[new_event], held=held)
