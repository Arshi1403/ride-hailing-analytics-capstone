"""Deduplication and row validation. Order in the pipeline: dedupe first (the latest copy wins),
then validate the winner. A rejected winner is not replaced by an older valid copy."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .schemas import UNIFIED_SCHEMA

MAX_TRIP_SECONDS = 4 * 3600
REJECT_REASONS = (
    "NULL_RIDE_ID", "NULL_PICKUP_TS", "NULL_DROPOFF_TS", "NULL_FARE", "NEGATIVE_FARE",
    "NEGATIVE_TRIP_DURATION", "TRIP_TOO_LONG",
)


def _row_hash(df: DataFrame) -> F.Column:
    parts = [F.coalesce(F.col(f.name).cast("string"), F.lit("\u0000")) for f in UNIFIED_SCHEMA.fields]
    return F.sha2(F.concat_ws("\u001f", *parts), 256)


def dedupe_by_ride_id(df: DataFrame) -> DataFrame:
    """Keep one row per ride_id: latest ingest_ts, then stream over historical, then larger source_file,
    then larger content hash. The result does not depend on input order. Rows with a null ride_id
    cannot be matched, so they pass through and are rejected later."""
    hashed = df.withColumn("_row_hash", _row_hash(df))
    order = Window.partitionBy("ride_id").orderBy(
        F.col("ingest_ts").desc_nulls_last(),
        F.when(F.col("source") == "stream", 0).otherwise(1).asc(),
        F.col("source_file").desc_nulls_last(),
        F.col("_row_hash").desc(),
    )
    winners = (
        hashed.where(F.col("ride_id").isNotNull())
        .withColumn("_rn", F.row_number().over(order))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
    unmatched = hashed.where(F.col("ride_id").isNull())
    return winners.unionByName(unmatched).drop("_row_hash")


def add_reject_reason(df: DataFrame) -> DataFrame:
    """First matching reason in REJECT_REASONS order; null means the row is valid."""
    trip_seconds = F.col("dropoff_ts").cast("double") - F.col("pickup_ts").cast("double")
    reason = (
        F.when(F.col("ride_id").isNull(), "NULL_RIDE_ID")
        .when(F.col("pickup_ts").isNull(), "NULL_PICKUP_TS")
        .when(F.col("dropoff_ts").isNull(), "NULL_DROPOFF_TS")
        .when(F.col("fare").isNull(), "NULL_FARE")
        .when(F.col("fare") < 0, "NEGATIVE_FARE")
        .when(trip_seconds < 0, "NEGATIVE_TRIP_DURATION")
        .when(trip_seconds > MAX_TRIP_SECONDS, "TRIP_TOO_LONG")
    )
    return df.withColumn("reject_reason", reason)


def split_valid_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    flagged = add_reject_reason(df)
    valid = flagged.where(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = flagged.where(F.col("reject_reason").isNotNull())
    return valid, rejected
