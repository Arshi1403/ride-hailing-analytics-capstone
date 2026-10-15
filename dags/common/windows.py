"""Pure functions used as Jinja macros and by task callables: pickup-date window, run id, forecast origin, object names."""
from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Mapping

MAX_BACKFILL_DAYS = 7
RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _param(params: Mapping[str, Any] | None, name: str, default: Any) -> Any:
    value = (params or {}).get(name, default)
    return default if value in (None, "") else value


def backfill_days(params: Mapping[str, Any] | None) -> int:
    """Number of pickup dates (ending at the run date) a manual backfill recomputes; 0 means the normal daily window."""
    raw = _param(params, "backfill_days", 0)
    try:
        days = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"backfill_days must be an integer from 0 to {MAX_BACKFILL_DAYS}, got {raw!r}") from None
    if not 0 <= days <= MAX_BACKFILL_DAYS:
        raise ValueError(f"backfill_days must be between 0 and {MAX_BACKFILL_DAYS}, got {days}")
    return days


def window_start(ds: str, params: Mapping[str, Any] | None, lookback_days: int) -> str:
    """First pickup date to recompute. Daily run: ds - lookback_days. Backfill of N days: the N dates ending at ds."""
    end = date.fromisoformat(ds)
    days = backfill_days(params)
    start = end - timedelta(days=days - 1) if days else end - timedelta(days=lookback_days)
    return start.isoformat()


def window_dates(start: str, end: str) -> list[str]:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last:
        raise ValueError(f"window start {start} is after window end {end}")
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def run_id(ds: str, ts_nodash: str) -> str:
    """Stable per DAG run (same value on task retries and clears), valid for Spark --run-id and BigQuery labels."""
    value = f"{ds}-{ts_nodash.lower()}"
    if not RUN_ID_RE.match(value):
        raise ValueError(f"cannot build a run id from ds={ds!r} ts_nodash={ts_nodash!r}")
    return value


def historical_object(ds: str, params: Mapping[str, Any] | None, template: str) -> str:
    override = _param(params, "historical_object", "")
    name = str(override) if override else template.format(ds=ds)
    if name.startswith("gs://") or name.startswith("/") or ".." in name.split("/"):
        raise ValueError(f"historical object must be a relative path inside the landing bucket, got {name!r}")
    return name


def as_of_ts(ds: str, business_tz: str) -> str:
    """Forecast origin: midnight after run date `ds` in the business timezone, floored to the UTC hour.
    The BQML SQL needs an hour-aligned UTC timestamp; half-hour offsets lose the last half hour of the day."""
    midnight = datetime.combine(date.fromisoformat(ds) + timedelta(days=1), time.min)
    if business_tz == "UTC":
        local = midnight.replace(tzinfo=timezone.utc)
    else:
        from zoneinfo import ZoneInfo  # needs tzdata, present on Composer workers

        local = midnight.replace(tzinfo=ZoneInfo(business_tz))
    utc = local.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return utc.strftime("%Y-%m-%d %H:%M:%S+00:00")


def make_macros(settings) -> dict:
    """Macros exposed to Jinja as user_defined_macros. Closures over settings keep templates short."""
    return {
        "rh_window_start": lambda ds, params=None: window_start(ds, params, settings.lookback_days),
        "rh_run_id": run_id,
        "rh_historical_object": lambda ds, params=None: historical_object(ds, params, settings.historical_object_template),
        "rh_as_of_ts": lambda ds: as_of_ts(ds, settings.business_tz),
    }
