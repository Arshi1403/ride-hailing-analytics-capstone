from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import types as T

from ridespark.derive import add_derived_columns
from ridespark.enrich import DimensionError, DimensionUnavailableError, enrich_drivers, enrich_zones, prepare_zones
from ridespark.scd2 import apply_snapshot, empty_history
from spark_helpers import D1, D2, ZONES, make_df, settle, snapshot_df, unified_df, unified_row, utc, zones_df


def derived(spark, tz="UTC", **overrides):
    return add_derived_columns(unified_df(spark, unified_row(**overrides)), tz).first()


def test_trip_minutes_and_pickup_date(spark):
    row = derived(spark)
    assert row.trip_minutes == 30.0 and row.pickup_date == date(2026, 10, 14)


def test_revenue_per_km_only_with_valid_distance(spark):
    assert derived(spark, distance_km=3.218688, fare=Decimal("10")).revenue_per_km == pytest.approx(3.1069, abs=1e-4)
    assert derived(spark, distance_km=None).revenue_per_km is None
    assert derived(spark, distance_km=0.0).revenue_per_km is None


@pytest.mark.parametrize("surge,flag", [(1.0, False), (1.01, True), (2.5, True), (None, None)])
def test_surge_flag(spark, surge, flag):
    assert derived(spark, surge_multiplier=surge).surge_flag is flag


@pytest.mark.parametrize("hour,bucket", [(0, "NIGHT"), (5, "NIGHT"), (6, "MORNING_PEAK"), (9, "MORNING_PEAK"),
                                         (10, "MIDDAY"), (15, "MIDDAY"), (16, "EVENING_PEAK"),
                                         (19, "EVENING_PEAK"), (20, "LATE_EVENING"), (23, "LATE_EVENING")])
def test_hour_buckets(spark, hour, bucket):
    assert derived(spark, pickup_ts=utc(2026, 10, 14, hour, 30), dropoff_ts=utc(2026, 10, 14, hour, 45)).hour_bucket == bucket


def test_weekday_name(spark):
    assert derived(spark).weekday == "Wed"  # 2026-10-14


def test_timezone_moves_date_weekday_and_bucket(spark):
    row = derived(spark, tz="Asia/Kolkata", pickup_ts=utc(2026, 10, 14, 20, 0), dropoff_ts=utc(2026, 10, 14, 20, 30))
    assert (row.pickup_date, row.weekday, row.hour_bucket) == (date(2026, 10, 15), "Thu", "NIGHT")


def test_zone_enrichment_matched_and_not_found(spark):
    zones = prepare_zones(zones_df(spark, *ZONES))
    out = {r.ride_id: r for r in enrich_zones(unified_df(
        spark, unified_row(ride_id="ok"), unified_row(ride_id="p", pickup_zone="99"),
        unified_row(ride_id="d", dropoff_zone="99"), unified_row(ride_id="b", pickup_zone="98", dropoff_zone="99"),
        unified_row(ride_id="n", pickup_zone=None)), zones).collect()}
    assert (out["ok"].zone_match, out["ok"].pickup_zone_name, out["ok"].dropoff_borough) == ("MATCHED", "Alpha", "Queens")
    assert out["p"].zone_match == "PICKUP_NOT_FOUND" and out["p"].pickup_zone_name is None
    assert out["d"].zone_match == "DROPOFF_NOT_FOUND" and out["d"].dropoff_zone_name is None
    assert out["b"].zone_match == "BOTH_NOT_FOUND"
    assert out["n"].zone_match == "PICKUP_NOT_FOUND"


def test_zone_dimension_unavailable_keeps_rows_with_null_attributes(spark):
    row = enrich_zones(unified_df(spark, unified_row()), None).first()
    assert row.zone_match == "DIMENSION_UNAVAILABLE" and row.pickup_zone_name is None


def test_zone_dimension_accepts_tlc_lookup_columns(spark):
    tlc = make_df(spark, [("1", "Manhattan", "Alpha", "Yellow Zone")], T.StructType(
        [T.StructField(n, T.StringType()) for n in ("LocationID", "Borough", "Zone", "service_zone")]))
    zone = prepare_zones(tlc).first()
    assert (zone.zone_id, zone.zone_name, zone.borough) == ("1", "Alpha", "Manhattan")


def test_zone_dimension_duplicates_and_empty_are_errors(spark):
    with pytest.raises(DimensionError, match="duplicated"):
        prepare_zones(zones_df(spark, *ZONES, ("1", "Other", "X", "Y")))
    with pytest.raises(DimensionUnavailableError):
        prepare_zones(zones_df(spark))


def history(spark):
    h = settle(spark, apply_snapshot(empty_history(spark), snapshot_df(spark, ("D1", "SEDAN", "1", "ACTIVE")), D1))
    return settle(spark, apply_snapshot(h, snapshot_df(spark, ("D1", "SUV", "1", "ACTIVE")), D2))


def enriched(spark, scd2, **overrides):
    rides = add_derived_columns(unified_df(spark, unified_row(**overrides)), "UTC")
    return enrich_drivers(rides, scd2).first()


def test_driver_enrichment_uses_the_version_valid_on_pickup_date(spark):
    h = history(spark)
    old = enriched(spark, h, pickup_ts=utc(2026, 10, 1, 9), dropoff_ts=utc(2026, 10, 1, 10))
    new = enriched(spark, h, pickup_ts=utc(2026, 10, 2, 9), dropoff_ts=utc(2026, 10, 2, 10))
    assert (old.driver_vehicle_type, old.driver_version_from, old.driver_match) == ("SEDAN", D1, "MATCHED")
    assert (new.driver_vehicle_type, new.driver_version_from) == ("SUV", D2)


def test_driver_before_first_version_unknown_driver_and_missing_id(spark):
    h = history(spark)
    before = enriched(spark, h, pickup_ts=utc(2026, 9, 30, 9), dropoff_ts=utc(2026, 9, 30, 10))
    unknown = enriched(spark, h, driver_id="D404")
    none = enriched(spark, h, driver_id=None)
    assert before.driver_match == "DRIVER_NOT_FOUND" and before.driver_vehicle_type is None
    assert unknown.driver_match == "DRIVER_NOT_FOUND"
    assert none.driver_match == "NO_DRIVER_ID"


def test_driver_dimension_unavailable(spark):
    row = enriched(spark, None)
    assert row.driver_match == "DIMENSION_UNAVAILABLE" and row.driver_vehicle_type is None


def test_enrichment_never_multiplies_rows(spark):
    h = history(spark)
    rides = add_derived_columns(unified_df(spark, *[unified_row(ride_id=f"r{i}") for i in range(5)]), "UTC")
    assert enrich_drivers(rides, h).count() == 5
