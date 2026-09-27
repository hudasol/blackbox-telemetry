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

    # Both remaining switches are drawn unconditionally here, every
    # tick, regardless of what the other one rolled. They used to be
    # checked with an early return on should_corrupt() — which meant a
    # tick where bad-data fired never even called should_reorder(),
    # silently suppressing SIM_OUT_OF_ORDER_RATE below its configured
    # value whenever SIM_BAD_DATA_RATE was also > 0 (and, by the same
    # bug in the other direction, would have suppressed
    # SIM_BAD_DATA_RATE too had the checks been ordered the other way
    # around). Drawing both every tick — before either one can
    # influence whether the other is even evaluated — is what actually
    # makes the four fault switches independent, as this module's own
    # docstring already claims. See
    # test_out_of_order_rate_is_unaffected_by_bad_data_also_firing and
    # test_bad_data_and_out_of_order_can_both_fire_on_the_same_tick in
    # tests/unit/test_simulator_scheduler.py.
    is_corrupt = injector.should_corrupt()
    wants_reorder = held is None and injector.should_reorder()

    # Bad-data and out-of-order are properties of the same event, not
    # mutually exclusive outcomes — if both fire on this tick, the
    # thing that eventually goes out (now, or later if it's held) is
    # the corrupted version.
    outgoing = injector.corrupt(new_event) if is_corrupt else new_event

    if wants_reorder:
        # Hold this one back; it'll be released only after a later
        # (higher-seq) event has already gone out, so it arrives at the
        # API genuinely out of order rather than just delayed.
        return TickPlan(to_send=[], held=outgoing)

    if held is not None:
        # Release the held (older-seq) event right after the current
        # (newer-seq) one — that ordering on the wire is the whole point.
        return TickPlan(to_send=[outgoing, held], held=None)

    return TickPlan(to_send=[outgoing], held=held)
