"""Deterministic mock ride-event generation (pure logic, no I/O)."""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta

from .schema import format_ts

# NYC TLC taxi zone ids, so streamed zones line up with the historical NYC Taxi data.
ZONES = tuple(str(i) for i in range(1, 266))
SURGE_CHOICES = (1.0,) * 6 + (1.25, 1.5, 2.0, 2.5, 3.0)
INVALID_KINDS = ("negative_fare", "surge_too_high", "bad_timestamp", "missing_driver", "fare_as_string")


def _zone_weights() -> list[float]:
    return [1.0 / (rank ** 0.5) for rank in range(1, len(ZONES) + 1)]


def generate_events(count: int, seed: int, start_time: datetime, duration_seconds: float) -> list[dict]:
    """Return `count` valid events with pickup_ts spread evenly over `duration_seconds`."""
    rng = random.Random(seed)
    weights = _zone_weights()
    step = duration_seconds / count
    events = []
    for i in range(count):
        pickup = start_time + timedelta(seconds=int(i * step))
        minutes = rng.randint(5, 60)
        surge = rng.choice(SURGE_CHOICES)
        fare = round((3.0 + 0.7 * minutes + rng.uniform(0, 5)) * surge, 2)
        events.append(
            {
                "ride_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                "driver_id": f"D{rng.randint(1, 200):05d}",
                "rider_id": f"R{rng.randint(1, 5000):06d}",
                "pickup_zone": rng.choices(ZONES, weights)[0],
                "dropoff_zone": rng.choice(ZONES),
                "pickup_ts": format_ts(pickup),
                "dropoff_ts": format_ts(pickup + timedelta(minutes=minutes)),
                "fare": fare,
                "surge_multiplier": surge,
            }
        )
    return events


def corrupt_event(event: dict, kind: str) -> dict:
    """Return a copy of `event` broken in one known way (for dead-letter testing)."""
    bad = dict(event)
    if kind == "negative_fare":
        bad["fare"] = -5.0
    elif kind == "surge_too_high":
        bad["surge_multiplier"] = 9.9
    elif kind == "bad_timestamp":
        bad["pickup_ts"] = "15/10/2026 10:00"
    elif kind == "missing_driver":
        del bad["driver_id"]
    elif kind == "fare_as_string":
        bad["fare"] = "12.50"
    else:
        raise ValueError(f"unknown invalid kind: {kind}")
    return bad


def inject_invalid(events: list[dict], count: int, seed: int) -> list[int]:
    """Corrupt `count` events in place at deterministic positions; return the positions."""
    if count == 0:
        return []
    rng = random.Random(f"invalid-{seed}")
    positions = sorted(rng.sample(range(len(events)), count))
    for n, pos in enumerate(positions):
        events[pos] = corrupt_event(events[pos], INVALID_KINDS[n % len(INVALID_KINDS)])
    return positions
