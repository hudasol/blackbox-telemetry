"""The telemetry event contract.

This is the one shape asset_id/api/worker/simulator all agree on. Changing
a field here is a contract change and should be reflected in docs/PLAN.md.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .constants import (
    BATTERY_PCT_MAX,
    BATTERY_PCT_MIN,
    HEADING_DEG_MAX,
    HEADING_DEG_MIN,
    LINK_QUALITY_PCT_MAX,
    LINK_QUALITY_PCT_MIN,
    MAX_CLOCK_SKEW_SECONDS,
    MIN_SEQ,
)


class AssetType(str, Enum):
    """The three asset kinds this fleet ships with today.

    A new asset_id (a fourth drone, say) never needs a code change — see
    docs/PLAN.md, problem framing. A genuinely new *type* of asset does,
    since the type is part of the documented contract, not just an id.
    """

    DRONE = "drone"
    ROVER = "rover"
    QUADRUPED = "quadruped"


class AssetMode(str, Enum):
    """Documented operating modes.

    Add a value here (and to docs/PLAN.md) to support a new mode — this is
    the "or another documented mode" the brief allows for.
    """

    IDLE = "idle"
    MOVING = "moving"
    RETURNING = "returning"
    CHARGING = "charging"


def _check_json_safe(value: Any, path: str) -> None:
    """Recursively reject values that parse fine as JSON but can never be
    written to Postgres: a NUL byte (0x00) is not representable in a
    Postgres TEXT/JSONB value at all — not even escaped — and a
    non-finite float (NaN/Infinity) is accepted by json.dumps but
    rejected by Postgres's strict jsonb parser. Both would otherwise
    pass Pydantic validation cleanly (extra="allow" fields are never
    type-checked) and only fail once the worker tries to store them —
    permanently, since a byte-identical retry fails identically every
    time (docs/PLAN.md, failure policy: this is the boundary that keeps
    a malformed extra field from ever reaching that point).
    """
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path}: must be a finite number (no NaN/Infinity)")
    elif isinstance(value, str):
        if "\x00" in value:
            raise ValueError(f"{path}: must not contain NUL bytes")
    elif isinstance(value, dict):
        for key, item in value.items():
            _check_json_safe(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _check_json_safe(item, f"{path}[{index}]")


class Position(BaseModel):
    """A synthetic local coordinate, in meters."""

    model_config = ConfigDict(extra="forbid")

    x_m: float
    y_m: float
    z_m: float

    @field_validator("x_m", "y_m", "z_m")
    @classmethod
    def _finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("position components must be finite numbers")
        return v


class TelemetryEvent(BaseModel):
    """One telemetry event as submitted by an asset (or the simulator).

    This is the wire contract for POST /api/v1/telemetry. Fields beyond
    the ones defined here are allowed and preserved as-is (stored, never
    interpreted) — see docs/PLAN.md, data contract.
    """

    model_config = ConfigDict(extra="allow", str_strip_whitespace=True)

    asset_id: Annotated[str, Field(min_length=1)]
    asset_type: AssetType
    seq: Annotated[int, Field(ge=MIN_SEQ)]
    timestamp_utc: datetime
    position: Position
    speed_mps: float
    heading_deg: Annotated[float, Field(ge=HEADING_DEG_MIN, le=HEADING_DEG_MAX)]
    battery_pct: Annotated[float, Field(ge=BATTERY_PCT_MIN, le=BATTERY_PCT_MAX)]
    temperature_c: float
    link_quality_pct: Annotated[
        float, Field(ge=LINK_QUALITY_PCT_MIN, le=LINK_QUALITY_PCT_MAX)
    ]
    mode: AssetMode

    @field_validator("asset_id")
    @classmethod
    def _asset_id_no_nul_bytes(cls, v: str) -> str:
        # Postgres TEXT columns cannot store a NUL byte under any
        # encoding — asyncpg raises a plain ValueError (not a
        # PostgresError) trying to send one over the wire, which the
        # worker's Postgres-retry handling doesn't recognize, and
        # retrying never helps since the id is unchanged. Reject it here
        # instead, at the one contract both api and worker validate
        # against, so a bad id is a normal 422/dead-letter, not a crash.
        if "\x00" in v:
            raise ValueError("asset_id must not contain NUL bytes")
        return v

    @field_validator("speed_mps", "temperature_c")
    @classmethod
    def _finite_number(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("must be a finite number (no NaN/Infinity)")
        return v

    @field_validator("timestamp_utc")
    @classmethod
    def _tz_aware_utc(cls, v: datetime) -> datetime:
        # A naive datetime is treated as UTC rather than silently guessing
        # the sender's offset — the contract requires UTC.
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _reject_future_clock_skew(self) -> "TelemetryEvent":
        now = datetime.now(timezone.utc)
        if self.timestamp_utc - now > timedelta(seconds=MAX_CLOCK_SKEW_SECONDS):
            raise ValueError(
                f"timestamp_utc is more than {MAX_CLOCK_SKEW_SECONDS}s ahead "
                "of the server clock"
            )
        return self

    @model_validator(mode="after")
    def _reject_unstorable_extra_fields(self) -> "TelemetryEvent":
        # extra="allow" means these fields are never type-checked above —
        # a sender can attach a NaN, an Infinity, or a NUL-byte string
        # under any key, and it parses as valid JSON and a valid model.
        # It only fails once the worker tries to store it (json.dumps
        # happily emits NaN/Infinity; Postgres's jsonb parser rejects
        # both that and an embedded NUL byte) — permanently, the same
        # way every retry, since nothing about the message changes.
        # Reject it here instead, before it's ever enqueued or acked.
        for key, value in (self.model_extra or {}).items():
            _check_json_safe(value, key)
        return self

    @property
    def idempotency_key(self) -> tuple[str, int]:
        """(asset_id, seq) — the unique identity of a logical event.

        This is the pair the worker's Postgres unique constraint is built
        on (see docs/PLAN.md, decision D1/D2).
        """
        return (self.asset_id, self.seq)

    def extra_fields(self) -> dict[str, Any]:
        """Fields the sender included beyond the documented contract.

        Stored as-is by the worker, never interpreted by this service.
        """
        return self.model_extra or {}
