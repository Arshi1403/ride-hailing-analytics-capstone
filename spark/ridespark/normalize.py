"""Bring streaming and historical raw data into one unified ride schema. Nothing is invented:
fields a source does not have stay null (historical has no driver, rider or surge; streaming has no distance)."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .schemas import HIST_RAW_SCHEMA, STREAM_RAW_SCHEMA, UNIFIED_SCHEMA, conform

MILES_TO_KM = 1.609344  # TLC trip_distance is in miles


def clean_id(name: str) -> Column:
    """Trim whitespace; empty text becomes null."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed == "", None).otherwise(trimmed)


def normalize_stream(df: DataFrame) -> DataFrame:
    s = conform(df, STREAM_RAW_SCHEMA, "streaming raw rides")
    return s.select(
        clean_id("ride_id").alias("ride_id"),
        F.lit("stream").alias("source"),
        F.lit(None).cast("string").alias("source_file"),
        clean_id("driver_id").alias("driver_id"),
        clean_id("rider_id").alias("rider_id"),
        clean_id("pickup_zone").alias("pickup_zone"),
        clean_id("dropoff_zone").alias("dropoff_zone"),
        "pickup_ts", "dropoff_ts", "fare", "surge_multiplier",
        F.lit(None).cast("double").alias("distance_km"),
        "ingest_ts",
    )


def _text(col: Column) -> Column:
    return F.coalesce(col.cast("string"), F.lit(""))


def normalize_historical(df: DataFrame) -> DataFrame:
    """TLC rows have no trip id, so ride_id is a deterministic hash of the trip's own fields.
    Two trips identical in every hashed field collapse into one."""
    h = conform(df, HIST_RAW_SCHEMA, "historical raw trips")
    pickup, dropoff = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    stamp = "yyyy-MM-dd HH:mm:ss.SSSSSS"
    key = F.concat_ws(
        "|", _text(F.col("VendorID")), _text(F.date_format(pickup, stamp)), _text(F.date_format(dropoff, stamp)),
        _text(F.col("PULocationID")), _text(F.col("DOLocationID")), _text(F.col("fare_amount")),
    )
    return h.select(
        F.concat(F.lit("H-"), F.sha2(key, 256)).alias("ride_id"),
        F.lit("historical").alias("source"),
        clean_id("source_file").alias("source_file"),
        F.lit(None).cast("string").alias("driver_id"),
        F.lit(None).cast("string").alias("rider_id"),
        F.col("PULocationID").cast("string").alias("pickup_zone"),
        F.col("DOLocationID").cast("string").alias("dropoff_zone"),
        pickup.alias("pickup_ts"), dropoff.alias("dropoff_ts"),
        F.col("fare_amount").alias("fare"),
        F.lit(None).cast("double").alias("surge_multiplier"),
        F.when(F.col("trip_distance") > 0, F.col("trip_distance") * F.lit(MILES_TO_KM)).alias("distance_km"),
        "ingest_ts",
    )


def build_unified(stream_df: DataFrame | None, hist_df: DataFrame | None) -> DataFrame:
    parts = []
    if stream_df is not None:
        parts.append(normalize_stream(stream_df))
    if hist_df is not None:
        parts.append(normalize_historical(hist_df))
    if not parts:
        raise ValueError("at least one of the streaming or historical inputs is required")
    unified = parts[0]
    for part in parts[1:]:
        unified = unified.unionByName(part)
    return unified.select([f.name for f in UNIFIED_SCHEMA.fields])
