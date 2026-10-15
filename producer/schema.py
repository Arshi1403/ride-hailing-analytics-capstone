"""Ride event schema and validation.

The JavaScript UDF in ingestion/udf/ride_event_udf.js mirrors these rules. Change both together.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = "1.0"

STRING_FIELDS = ("ride_id", "driver_id", "rider_id", "pickup_zone", "dropoff_zone")
TIMESTAMP_FIELDS = ("pickup_ts", "dropoff_ts")
NUMBER_FIELDS = ("fare", "surge_multiplier")
FIELDS = STRING_FIELDS + TIMESTAMP_FIELDS + NUMBER_FIELDS

ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

FARE_MAX = 1000.0
SURGE_MIN = 1.0
SURGE_MAX = 5.0
MAX_TRIP_SECONDS = 6 * 3600


@dataclass(frozen=True)
class Violation:
    field: str
    code: str
    message: str


class EventValidationError(ValueError):
    def __init__(self, violations: list[Violation]):
        self.violations = violations
        super().__init__("; ".join(f"{v.field}: {v.code} ({v.message})" for v in violations))


def parse_ts(value: str) -> datetime:
    """Parse strict `YYYY-MM-DDTHH:MM:SSZ` (UTC). Raises ValueError when malformed."""
    if not isinstance(value, str) or not TS_RE.match(value):
        raise ValueError(f"expected YYYY-MM-DDTHH:MM:SSZ, got {value!r}")
    return datetime.strptime(value, TS_FORMAT).replace(tzinfo=timezone.utc)


def format_ts(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(TS_FORMAT)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def validate_event(payload: Any) -> list[Violation]:
    """Return all violations; an empty list means the event is valid."""
    if not isinstance(payload, dict):
        return [Violation("<root>", "BAD_TYPE", "payload must be a JSON object")]

    violations: list[Violation] = []
    for name in payload:
        if name not in FIELDS:
            violations.append(Violation(name, "UNKNOWN_FIELD", "field is not in the schema"))

    for name in FIELDS:
        if name not in payload or payload[name] is None:
            violations.append(Violation(name, "MISSING_FIELD", "required field is missing or null"))

    for name in STRING_FIELDS:
        value = payload.get(name)
        if value is None:
            continue
        if not isinstance(value, str):
            violations.append(Violation(name, "BAD_TYPE", "must be a string"))
        elif not ID_RE.match(value):
            violations.append(Violation(name, "BAD_ID", "must match [A-Za-z0-9_-]{1,64}"))

    parsed: dict[str, datetime] = {}
    for name in TIMESTAMP_FIELDS:
        value = payload.get(name)
        if value is None:
            continue
        try:
            parsed[name] = parse_ts(value)
        except ValueError as exc:
            violations.append(Violation(name, "BAD_TIMESTAMP", str(exc)))

    if len(parsed) == 2:
        seconds = (parsed["dropoff_ts"] - parsed["pickup_ts"]).total_seconds()
        if seconds < 0:
            violations.append(Violation("dropoff_ts", "DROPOFF_BEFORE_PICKUP", "must not precede pickup_ts"))
        elif seconds > MAX_TRIP_SECONDS:
            violations.append(Violation("dropoff_ts", "TRIP_TOO_LONG", f"trip exceeds {MAX_TRIP_SECONDS} seconds"))

    fare = payload.get("fare")
    if fare is not None:
        if not _is_number(fare):
            violations.append(Violation("fare", "BAD_TYPE", "must be a finite number"))
        elif not 0 < fare <= FARE_MAX:
            violations.append(Violation("fare", "FARE_OUT_OF_RANGE", f"must be > 0 and <= {FARE_MAX}"))

    surge = payload.get("surge_multiplier")
    if surge is not None:
        if not _is_number(surge):
            violations.append(Violation("surge_multiplier", "BAD_TYPE", "must be a finite number"))
        elif not SURGE_MIN <= surge <= SURGE_MAX:
            violations.append(
                Violation("surge_multiplier", "SURGE_OUT_OF_RANGE", f"must be between {SURGE_MIN} and {SURGE_MAX}")
            )

    return violations


def ensure_valid(payload: Any) -> None:
    violations = validate_event(payload)
    if violations:
        raise EventValidationError(violations)
