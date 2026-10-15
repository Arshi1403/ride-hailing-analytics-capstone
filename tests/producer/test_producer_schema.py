import copy
from datetime import datetime, timezone

import pytest

from producer.generator import generate_events
from producer.schema import EventValidationError, FIELDS, ensure_valid, parse_ts, validate_event

START = datetime(2026, 10, 15, 10, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def event():
    return generate_events(1, 1, START, 60)[0]


def codes(payload):
    return {(v.field, v.code) for v in validate_event(payload)}


def test_generated_event_is_valid(event):
    assert validate_event(event) == []
    assert set(event) == set(FIELDS)


@pytest.mark.parametrize("field", FIELDS)
def test_missing_field_is_reported(event, field):
    del event[field]
    assert (field, "MISSING_FIELD") in codes(event)


def test_null_field_is_missing(event):
    event["driver_id"] = None
    assert ("driver_id", "MISSING_FIELD") in codes(event)


def test_unknown_field_rejected(event):
    event["ingest_ts"] = "2026-10-15T10:00:00Z"
    assert ("ingest_ts", "UNKNOWN_FIELD") in codes(event)


@pytest.mark.parametrize("value", ["15/10/2026 10:00", "2026-10-15 10:00:00", "2026-10-15T10:00:00+00:00",
                                   "2026-13-15T10:00:00Z", "2026-02-30T10:00:00Z", 12345])
def test_bad_timestamp_formats(event, value):
    event["pickup_ts"] = value
    assert ("pickup_ts", "BAD_TIMESTAMP") in codes(event)


def test_dropoff_before_pickup(event):
    event["dropoff_ts"] = "2000-01-01T00:00:00Z"
    assert ("dropoff_ts", "DROPOFF_BEFORE_PICKUP") in codes(event)


def test_trip_too_long(event):
    event["pickup_ts"] = "2026-10-15T00:00:00Z"
    event["dropoff_ts"] = "2026-10-15T07:00:00Z"
    assert ("dropoff_ts", "TRIP_TOO_LONG") in codes(event)


@pytest.mark.parametrize("fare,code", [(0, "FARE_OUT_OF_RANGE"), (-1, "FARE_OUT_OF_RANGE"),
                                       (1000.01, "FARE_OUT_OF_RANGE"), ("12.5", "BAD_TYPE"),
                                       (True, "BAD_TYPE"), (float("nan"), "BAD_TYPE")])
def test_bad_fares(event, fare, code):
    event["fare"] = fare
    assert ("fare", code) in codes(event)


@pytest.mark.parametrize("surge,code", [(0.99, "SURGE_OUT_OF_RANGE"), (5.01, "SURGE_OUT_OF_RANGE"),
                                        ("2", "BAD_TYPE"), (None, "MISSING_FIELD")])
def test_bad_surge(event, surge, code):
    event["surge_multiplier"] = surge
    assert ("surge_multiplier", code) in codes(event)


@pytest.mark.parametrize("fare,surge", [(0.01, 1), (1000, 5), (25, 1.0)])
def test_boundary_values_are_valid(event, fare, surge):
    event["fare"], event["surge_multiplier"] = fare, surge
    assert validate_event(event) == []


def test_bad_id_and_type(event):
    event["ride_id"] = "has space"
    event["rider_id"] = 7
    found = codes(event)
    assert ("ride_id", "BAD_ID") in found and ("rider_id", "BAD_TYPE") in found


def test_non_object_payload():
    assert validate_event([1, 2])[0].code == "BAD_TYPE"


def test_ensure_valid_raises_with_all_violations(event):
    bad = copy.deepcopy(event)
    bad["fare"] = -1
    del bad["driver_id"]
    with pytest.raises(EventValidationError) as info:
        ensure_valid(bad)
    assert len(info.value.violations) == 2


def test_parse_ts_roundtrip():
    assert parse_ts("2026-10-15T10:00:00Z") == START
