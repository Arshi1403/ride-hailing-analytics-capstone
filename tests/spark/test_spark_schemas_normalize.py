from decimal import Decimal

import pytest
from pyspark.sql import functions as F, types as T

from ridespark.normalize import build_unified, normalize_historical, normalize_stream
from ridespark.schemas import (
    CURATED_RIDES_SCHEMA, HIST_RAW_SCHEMA, STREAM_RAW_SCHEMA, UNIFIED_SCHEMA, SchemaMismatchError,
    bigquery_schema, conform, signature,
)
from spark_helpers import hist_df, hist_row, make_df, stream_df, stream_row, utc


def test_streaming_and_historical_normalize_to_identical_schema(spark):
    s = normalize_stream(stream_df(spark, stream_row()))
    h = normalize_historical(hist_df(spark, hist_row()))
    assert signature(s) == signature(h) == signature(UNIFIED_SCHEMA)


def test_conform_accepts_extra_columns_and_widens_integers(spark):
    df = make_df(spark, [(1, 2.5, "x")], T.StructType([
        T.StructField("VendorID", T.IntegerType()), T.StructField("trip_distance", T.DoubleType()),
        T.StructField("ignored", T.StringType())]))
    out = conform(df, T.StructType([T.StructField("VendorID", T.LongType()), T.StructField("trip_distance", T.DoubleType())]), "t")
    assert signature(out) == [("VendorID", "bigint"), ("trip_distance", "double")]


def test_missing_columns_are_reported_by_name(spark):
    df = stream_df(spark, stream_row()).drop("ride_id", "fare")
    with pytest.raises(SchemaMismatchError, match=r"ride_id.*fare"):
        normalize_stream(df)


def test_wrong_types_are_reported_not_silently_cast(spark):
    df = stream_df(spark, stream_row()).withColumn("pickup_ts", F.col("pickup_ts").cast("string"))
    with pytest.raises(SchemaMismatchError, match="pickup_ts: found string, expected timestamp"):
        normalize_stream(df)


def test_historical_wrong_type_is_rejected(spark):
    df = hist_df(spark, hist_row()).withColumn("PULocationID", F.lit("abc"))
    with pytest.raises(SchemaMismatchError, match="PULocationID"):
        normalize_historical(df)


def test_streaming_and_historical_schema_mismatch_between_each_other(spark):
    # a historical frame fed into the streaming normalizer must fail, not produce nulls
    with pytest.raises(SchemaMismatchError):
        normalize_stream(hist_df(spark, hist_row()))


def test_stream_fields_pass_through_and_distance_is_not_invented(spark):
    row = normalize_stream(stream_df(spark, stream_row(driver_id="  D9 ", rider_id=" "))).first()
    assert row.source == "stream" and row.source_file is None
    assert row.driver_id == "D9" and row.rider_id is None
    assert row.distance_km is None
    assert row.fare == Decimal("25.50")


def test_historical_has_no_driver_rider_or_surge(spark):
    row = normalize_historical(hist_df(spark, hist_row())).first()
    assert (row.driver_id, row.rider_id, row.surge_multiplier) == (None, None, None)
    assert row.source == "historical" and row.pickup_zone == "1" and row.dropoff_zone == "2"


def test_historical_distance_is_converted_from_miles_only_when_valid(spark):
    rows = normalize_historical(hist_df(
        spark, hist_row(trip_distance=2.0), hist_row(trip_distance=0.0, PULocationID=3),
        hist_row(trip_distance=None, PULocationID=4), hist_row(trip_distance=-1.0, PULocationID=5),
    )).collect()
    by_zone = {r.pickup_zone: r.distance_km for r in rows}
    assert by_zone["1"] == pytest.approx(3.218688)
    assert by_zone["3"] is None and by_zone["4"] is None and by_zone["5"] is None


def test_historical_ride_id_is_deterministic_and_content_based(spark):
    a = normalize_historical(hist_df(spark, hist_row(source_file="gs://bkt/a_2018.csv"))).first().ride_id
    b = normalize_historical(hist_df(spark, hist_row(source_file="gs://bkt/b_2018.csv"))).first().ride_id
    c = normalize_historical(hist_df(spark, hist_row(fare_amount=Decimal("11.00")))).first().ride_id
    assert a == b and a.startswith("H-") and len(a) == 66
    assert a != c


def test_build_unified_unions_both_sources(spark):
    out = build_unified(stream_df(spark, stream_row()), hist_df(spark, hist_row()))
    assert {r.source for r in out.collect()} == {"stream", "historical"}


def test_build_unified_requires_an_input():
    with pytest.raises(ValueError):
        build_unified(None, None)


def test_bigquery_schema_mapping_for_curated_output():
    columns = {c["name"]: c["type"] for c in bigquery_schema(CURATED_RIDES_SCHEMA)}
    assert columns["fare"] == "NUMERIC" and columns["pickup_ts"] == "TIMESTAMP"
    assert columns["pickup_date"] == "DATE" and columns["surge_flag"] == "BOOL"
    assert columns["trip_minutes"] == "FLOAT64" and columns["ride_id"] == "STRING"
    assert list(columns)[-1] == "pickup_date"


def test_input_schemas_are_explicit():
    assert [f.name for f in STREAM_RAW_SCHEMA.fields][-1] == "ingest_ts"
    assert "tpep_pickup_datetime" in HIST_RAW_SCHEMA.fieldNames()
