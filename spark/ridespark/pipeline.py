"""Compose the transformations. All functions are DataFrame -> DataFrame; I/O lives in io.py and job.py."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .derive import add_derived_columns, pickup_date_col
from .enrich import enrich_drivers, enrich_zones
from .quality import dedupe_by_ride_id, split_valid_rejected
from .schemas import CURATED_RIDES_SCHEMA, REJECTED_SCHEMA


@dataclass(frozen=True)
class RunContext:
    processing_date: date  # the run's date (audit column); NOT where a row is stored
    pickup_start: date  # first pickup_date to (re)compute, inclusive
    pickup_end: date  # last pickup_date to (re)compute, inclusive
    timezone: str
    run_id: str


def add_event_date(df: DataFrame, ctx: RunContext) -> DataFrame:
    """event_date places a row in a window and in the quarantine partition: its pickup date, else its
    ingest date, else the processing date (so rows with a null pickup are still reported)."""
    ingest_date = F.to_date(F.from_utc_timestamp(F.col("ingest_ts"), ctx.timezone))
    return df.withColumn(
        "event_date", F.coalesce(pickup_date_col(ctx.timezone), ingest_date, F.lit(ctx.processing_date))
    )


def select_window(df: DataFrame, ctx: RunContext) -> DataFrame:
    return df.where(F.col("event_date").between(F.lit(ctx.pickup_start), F.lit(ctx.pickup_end)))


@dataclass(frozen=True)
class RideResult:
    curated: DataFrame
    rejected: DataFrame
    windowed: DataFrame
    deduped: DataFrame


def process_rides(
    unified: DataFrame, zones: DataFrame | None, drivers_scd2: DataFrame | None, ctx: RunContext
) -> RideResult:
    """Output of a pickup_date depends only on the raw rows of that date, so rerunning a window
    reproduces the same partitions."""
    windowed = select_window(add_event_date(unified, ctx), ctx)
    deduped = dedupe_by_ride_id(windowed)
    valid, rejected = split_valid_rejected(deduped)

    curated = enrich_drivers(enrich_zones(add_derived_columns(valid, ctx.timezone), zones), drivers_scd2)
    curated = (
        curated.withColumn("processing_date", F.lit(ctx.processing_date))
        .withColumn("run_id", F.lit(ctx.run_id))
        .select([f.name for f in CURATED_RIDES_SCHEMA.fields])
    )
    rejected = (
        rejected.withColumn("processing_date", F.lit(ctx.processing_date))
        .withColumn("run_id", F.lit(ctx.run_id))
        .select([f.name for f in REJECTED_SCHEMA.fields])
    )
    return RideResult(curated, rejected, windowed, deduped)


def compute_metrics(result: RideResult) -> dict:
    def counts(df: DataFrame, column: str) -> dict:
        return {row[column]: row["count"] for row in df.groupBy(column).count().collect()}

    windowed_count = result.windowed.count()
    deduped_count = result.deduped.count()
    return {
        "rows_in_window": windowed_count,
        "duplicates_removed": windowed_count - deduped_count,
        "curated_rows": result.curated.count(),
        "rejected_rows": result.rejected.count(),
        "rejected_by_reason": counts(result.rejected, "reject_reason"),
        "curated_by_source": counts(result.curated, "source"),
        "zone_match": counts(result.curated, "zone_match"),
        "driver_match": counts(result.curated, "driver_match"),
    }
