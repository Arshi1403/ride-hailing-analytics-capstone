"""Zone and driver enrichment. A missing dimension never produces invented values:
attributes stay null and a *_match column says why (see spark/README.md)."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .schemas import ZONE_COLUMN_ALIASES, ZONE_SCHEMA, conform


class DimensionError(ValueError):
    pass


class DimensionUnavailableError(DimensionError):
    """A required dimension could not be read and the run was not told to continue without it."""


def prepare_zones(zones_df: DataFrame) -> DataFrame:
    renamed = zones_df
    for old, new in ZONE_COLUMN_ALIASES.items():
        if old in renamed.columns and new not in renamed.columns:
            renamed = renamed.withColumnRenamed(old, new)
    if "service_zone" not in renamed.columns:
        renamed = renamed.withColumn("service_zone", F.lit(None).cast("string"))
    zones = conform(renamed, ZONE_SCHEMA, "zone dimension")
    zones = zones.withColumn("zone_id", F.trim(F.col("zone_id"))).where(F.col("zone_id").isNotNull() & (F.col("zone_id") != ""))
    duplicated = zones.groupBy("zone_id").count().where(F.col("count") > 1).count()
    if duplicated:
        raise DimensionError(f"zone dimension has {duplicated} duplicated zone_id value(s)")
    if zones.limit(1).count() == 0:
        raise DimensionUnavailableError("zone dimension is empty")
    return zones


def enrich_zones(df: DataFrame, zones: DataFrame | None) -> DataFrame:
    if zones is None:
        return (
            df.withColumn("pickup_zone_name", F.lit(None).cast("string"))
            .withColumn("pickup_borough", F.lit(None).cast("string"))
            .withColumn("dropoff_zone_name", F.lit(None).cast("string"))
            .withColumn("dropoff_borough", F.lit(None).cast("string"))
            .withColumn("zone_match", F.lit("DIMENSION_UNAVAILABLE"))
        )
    pickup = zones.select(F.col("zone_id").alias("_p_id"), F.col("zone_name").alias("pickup_zone_name"),
                          F.col("borough").alias("pickup_borough"))
    dropoff = zones.select(F.col("zone_id").alias("_d_id"), F.col("zone_name").alias("dropoff_zone_name"),
                           F.col("borough").alias("dropoff_borough"))
    joined = (
        df.join(F.broadcast(pickup), df["pickup_zone"] == F.col("_p_id"), "left")
        .join(F.broadcast(dropoff), df["dropoff_zone"] == F.col("_d_id"), "left")
    )
    p_ok, d_ok = F.col("_p_id").isNotNull(), F.col("_d_id").isNotNull()
    match = (
        F.when(p_ok & d_ok, "MATCHED").when(d_ok, "PICKUP_NOT_FOUND").when(p_ok, "DROPOFF_NOT_FOUND")
        .otherwise("BOTH_NOT_FOUND")
    )
    return joined.withColumn("zone_match", match).drop("_p_id", "_d_id")


def enrich_drivers(df: DataFrame, scd2: DataFrame | None) -> DataFrame:
    """Point-in-time join: the driver version valid on the ride's pickup_date.
    Rides before a driver's first version get DRIVER_NOT_FOUND (history is not back-filled)."""
    if scd2 is None:
        return (
            df.withColumn("driver_vehicle_type", F.lit(None).cast("string"))
            .withColumn("driver_home_zone", F.lit(None).cast("string"))
            .withColumn("driver_status", F.lit(None).cast("string"))
            .withColumn("driver_version_from", F.lit(None).cast("date"))
            .withColumn("driver_match", F.lit("DIMENSION_UNAVAILABLE"))
        )
    dim = scd2.select(
        F.col("driver_id").alias("_dim_driver_id"),
        F.col("vehicle_type").alias("driver_vehicle_type"),
        F.col("home_zone").alias("driver_home_zone"),
        F.col("status").alias("driver_status"),
        F.col("effective_from").alias("driver_version_from"),
        F.col("effective_to").alias("_dim_to"),
    )
    cond = (
        (df["driver_id"] == F.col("_dim_driver_id"))
        & (df["pickup_date"] >= F.col("driver_version_from"))
        & (df["pickup_date"] < F.col("_dim_to"))
    )
    joined = df.join(F.broadcast(dim), cond, "left")
    match = (
        F.when(F.col("driver_id").isNull(), "NO_DRIVER_ID")
        .when(F.col("driver_version_from").isNotNull(), "MATCHED")
        .otherwise("DRIVER_NOT_FOUND")
    )
    return joined.withColumn("driver_match", match).drop("_dim_driver_id", "_dim_to")
