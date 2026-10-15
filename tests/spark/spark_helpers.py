"""Test data builders. Timestamps are timezone-aware UTC so results do not depend on the machine's time zone.
DataFrames are built through pandas/Arrow: on Windows every plain createDataFrame starts slow Python workers."""
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd
from pyspark.sql import types as T

from ridespark.schemas import (
    DRIVER_SNAPSHOT_SCHEMA, HIST_RAW_SCHEMA, STREAM_RAW_SCHEMA, UNIFIED_SCHEMA, ZONE_SCHEMA,
)


def make_df(spark, rows, schema):
    rows = list(rows)
    if not rows:
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    columns = {}
    for i, field in enumerate(schema.fields):
        series = pd.Series([row[i] for row in rows], dtype="object")
        columns[field.name] = pd.to_datetime(series, utc=True) if isinstance(field.dataType, T.TimestampType) else series
    return spark.createDataFrame(pd.DataFrame(columns), schema)


def utc(y, m, d, h=0, mi=0, s=0):
    return datetime(y, m, d, h, mi, s, tzinfo=timezone.utc)


def _row(schema, defaults, overrides):
    unknown = set(overrides) - set(defaults)
    assert not unknown, f"unknown fields {unknown}"
    merged = {**defaults, **overrides}
    return tuple(merged[f.name] for f in schema.fields)


STREAM_DEFAULTS = dict(
    ride_id="r1", driver_id="D1", rider_id="R1", pickup_zone="1", dropoff_zone="2",
    pickup_ts=utc(2026, 10, 14, 10, 0), dropoff_ts=utc(2026, 10, 14, 10, 30),
    fare=Decimal("25.50"), surge_multiplier=1.0, ingest_ts=utc(2026, 10, 14, 10, 31),
)
HIST_DEFAULTS = dict(
    VendorID=1, tpep_pickup_datetime=utc(2018, 3, 5, 8, 0), tpep_dropoff_datetime=utc(2018, 3, 5, 8, 20),
    trip_distance=2.0, PULocationID=1, DOLocationID=2, fare_amount=Decimal("10.00"),
    source_file="gs://bkt/yellow_tripdata_2018-03.csv", ingest_ts=utc(2026, 10, 14, 9, 0),
)
UNIFIED_DEFAULTS = dict(
    ride_id="r1", source="stream", source_file=None, driver_id="D1", rider_id="R1", pickup_zone="1",
    dropoff_zone="2", pickup_ts=utc(2026, 10, 14, 10, 0), dropoff_ts=utc(2026, 10, 14, 10, 30),
    fare=Decimal("25.50"), surge_multiplier=1.0, distance_km=None, ingest_ts=utc(2026, 10, 14, 10, 31),
)


def stream_row(**kw):
    return _row(STREAM_RAW_SCHEMA, STREAM_DEFAULTS, kw)


def hist_row(**kw):
    return _row(HIST_RAW_SCHEMA, HIST_DEFAULTS, kw)


def unified_row(**kw):
    return _row(UNIFIED_SCHEMA, UNIFIED_DEFAULTS, kw)


def stream_df(spark, *rows):
    return make_df(spark, rows, STREAM_RAW_SCHEMA)


def hist_df(spark, *rows):
    return make_df(spark, rows, HIST_RAW_SCHEMA)


def unified_df(spark, *rows):
    return make_df(spark, rows, UNIFIED_SCHEMA)


def zones_df(spark, *rows):
    return make_df(spark, rows, ZONE_SCHEMA)


def snapshot_df(spark, *rows):
    """rows: (driver_id, vehicle_type, home_zone, status)"""
    return make_df(spark, rows, DRIVER_SNAPSHOT_SCHEMA)


def settle(spark, df):
    """Cut the lineage of a small DataFrame. Chained apply_snapshot calls otherwise re-expand the whole
    plan at every step (the job applies a single snapshot per run). Tests only: localCheckpoint is not fault tolerant."""
    return df.localCheckpoint()


ZONES = (("1", "Alpha", "Manhattan", "Yellow Zone"), ("2", "Beta", "Queens", "Boro Zone"))
D1, D2, D3 = date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)
