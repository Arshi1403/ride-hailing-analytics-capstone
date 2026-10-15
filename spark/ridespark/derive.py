"""Derived analytics columns for valid rides."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def local_pickup(tz: str) -> Column:
    """Pickup wall-clock time in `tz`. Requires spark.sql.session.timeZone=UTC (set by the job)."""
    return F.from_utc_timestamp(F.col("pickup_ts"), tz)


def pickup_date_col(tz: str) -> Column:
    return F.to_date(local_pickup(tz))


def add_derived_columns(df: DataFrame, tz: str) -> DataFrame:
    """Adds pickup_date, trip_minutes, revenue_per_km, surge_flag, weekday, hour_bucket.

    revenue_per_km is only set when distance_km exists and is > 0 (streaming rides have no distance).
    surge_flag is null when the source has no surge value (historical rides).
    hour_bucket (local pickup hour): NIGHT 00-05, MORNING_PEAK 06-09, MIDDAY 10-15, EVENING_PEAK 16-19, LATE_EVENING 20-23.
    """
    local = local_pickup(tz)
    hour = F.hour(local)
    seconds = F.col("dropoff_ts").cast("double") - F.col("pickup_ts").cast("double")
    return (
        df.withColumn("pickup_date", F.to_date(local))
        .withColumn("trip_minutes", F.round(seconds / 60.0, 2))
        .withColumn("revenue_per_km",
                    F.when(F.col("distance_km") > 0, F.round(F.col("fare").cast("double") / F.col("distance_km"), 4)))
        .withColumn("surge_flag", F.col("surge_multiplier") > 1.0)
        .withColumn("weekday", F.date_format(local, "E"))
        .withColumn(
            "hour_bucket",
            F.when(hour < 6, "NIGHT").when(hour < 10, "MORNING_PEAK").when(hour < 16, "MIDDAY")
            .when(hour < 20, "EVENING_PEAK").otherwise("LATE_EVENING"),
        )
    )
