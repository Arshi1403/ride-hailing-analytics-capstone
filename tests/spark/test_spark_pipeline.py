from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from ridespark.enrich import prepare_zones
from ridespark.normalize import build_unified
from ridespark.pipeline import RunContext, compute_metrics, process_rides
from ridespark.scd2 import apply_snapshot, empty_history
from ridespark.schemas import CURATED_RIDES_SCHEMA, REJECTED_SCHEMA, signature
from spark_helpers import D1, ZONES, hist_df, hist_row, settle, snapshot_df, stream_df, stream_row, utc, zones_df

CTX = RunContext(date(2026, 10, 15), date(2026, 10, 12), date(2026, 10, 15), "UTC", "run-1")


def mixed_input(spark):
    stream = stream_df(
        spark,
        stream_row(ride_id="ok"),
        stream_row(ride_id="neg", fare=Decimal("-1")),
        stream_row(ride_id="nopick", pickup_ts=None),
        stream_row(ride_id="long", dropoff_ts=utc(2026, 10, 14, 15, 1)),
        stream_row(ride_id="dup", fare=Decimal("5"), ingest_ts=utc(2026, 10, 14, 10, 40)),
        stream_row(ride_id="dup", fare=Decimal("6"), ingest_ts=utc(2026, 10, 14, 10, 50)),
        stream_row(ride_id="old", pickup_ts=utc(2026, 9, 1, 10), dropoff_ts=utc(2026, 9, 1, 10, 30),
                   ingest_ts=utc(2026, 9, 1, 11)),  # outside the window
    )
    hist = hist_df(spark, hist_row(tpep_pickup_datetime=utc(2026, 10, 13, 8), tpep_dropoff_datetime=utc(2026, 10, 13, 8, 20),
                                   ingest_ts=utc(2026, 10, 13, 9)))
    return build_unified(stream, hist)


def drivers(spark):
    return settle(spark, apply_snapshot(empty_history(spark), snapshot_df(spark, ("D1", "SEDAN", "1", "ACTIVE")), D1))


def test_pipeline_splits_valid_rejected_and_duplicates(spark):
    result = process_rides(mixed_input(spark), prepare_zones(zones_df(spark, *ZONES)), drivers(spark), CTX)
    curated = {r.ride_id: r for r in result.curated.collect()}
    assert sorted(k for k in curated if not k.startswith("H-")) == ["dup", "ok"]
    assert curated["dup"].fare == Decimal("6")  # latest ingest_ts wins
    assert any(k.startswith("H-") for k in curated)
    rejected = {r.ride_id: r.reject_reason for r in result.rejected.collect()}
    assert rejected == {"neg": "NEGATIVE_FARE", "nopick": "NULL_PICKUP_TS", "long": "TRIP_TOO_LONG"}
    metrics = compute_metrics(result)
    assert metrics["rows_in_window"] == 7 and metrics["duplicates_removed"] == 1
    assert metrics["rejected_by_reason"]["TRIP_TOO_LONG"] == 1
    assert metrics["driver_match"] == {"MATCHED": 2, "NO_DRIVER_ID": 1}


def test_curated_and_rejected_schemas_equal_the_published_contract(spark):
    result = process_rides(mixed_input(spark), prepare_zones(zones_df(spark, *ZONES)), drivers(spark), CTX)
    assert signature(result.curated) == signature(CURATED_RIDES_SCHEMA)
    assert signature(result.rejected) == signature(REJECTED_SCHEMA)


def test_processing_date_is_an_audit_column_separate_from_pickup_date(spark):
    result = process_rides(mixed_input(spark), None, None, CTX)
    row = result.curated.where(F.col("ride_id") == "ok").first()
    assert row.pickup_date == date(2026, 10, 14) and row.processing_date == date(2026, 10, 15)
    assert row.run_id == "run-1"


def test_rejected_rows_without_pickup_are_partitioned_by_ingest_date(spark):
    result = process_rides(mixed_input(spark), None, None, CTX)
    assert result.rejected.where(F.col("ride_id") == "nopick").first().event_date == date(2026, 10, 14)


def test_rerunning_the_pipeline_on_the_same_input_gives_identical_output(spark):
    args = (mixed_input(spark), prepare_zones(zones_df(spark, *ZONES)), drivers(spark), CTX)
    first = sorted(tuple(map(str, r)) for r in process_rides(*args).curated.collect())
    second = sorted(tuple(map(str, r)) for r in process_rides(*args).curated.collect())
    assert first == second


def test_window_limits_which_pickup_dates_are_recomputed(spark):
    narrow = RunContext(date(2026, 10, 15), date(2026, 10, 14), date(2026, 10, 14), "UTC", "run-2")
    result = process_rides(mixed_input(spark), None, None, narrow)
    assert {r.pickup_date for r in result.curated.collect()} == {date(2026, 10, 14)}
