"""Reading and writing. Sources are `bq://project.dataset.table` (BigQuery connector), CSV, or Parquet
(local path or gs://). Outputs are Parquet under one base path."""
from __future__ import annotations

import json
from datetime import date

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import DRIVER_SCD2_SCHEMA, conform


def join_path(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/\\").replace("\\", "/"), *parts])


def read_source(spark: SparkSession, spec: str) -> DataFrame:
    if spec.startswith("bq://"):
        return spark.read.format("bigquery").option("table", spec[len("bq://"):]).load()
    if spec.lower().endswith(".csv"):
        return spark.read.option("header", True).csv(spec)  # all columns as text; schemas are checked afterwards
    return spark.read.parquet(spec)


def read_latest_scd2(spark: SparkSession, base: str) -> DataFrame | None:
    """Latest full driver-history state under <base>/dim_driver_scd2, or None if there is none yet."""
    try:
        states = spark.read.parquet(join_path(base, "dim_driver_scd2"))
    except AnalysisException:
        return None
    latest = states.agg(F.max("snapshot_date")).first()[0]
    if latest is None:
        return None
    return conform(states.where(F.col("snapshot_date") == F.lit(latest)).drop("snapshot_date"),
                   DRIVER_SCD2_SCHEMA, "stored driver history")


def materialize(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Copy a small DataFrame into driver memory so its source files can be overwritten safely."""
    return spark.createDataFrame(df.collect(), df.schema)


def write_rides(df: DataFrame, base: str) -> None:
    df.repartition("pickup_date").write.mode("overwrite").partitionBy("pickup_date").parquet(join_path(base, "rides"))


def write_rejected(df: DataFrame, base: str) -> None:
    df.repartition("event_date").write.mode("overwrite").partitionBy("event_date").parquet(join_path(base, "rejected"))


def write_scd2(df: DataFrame, base: str, snapshot_date: date) -> None:
    (df.withColumn("snapshot_date", F.lit(snapshot_date)).coalesce(1)
       .write.mode("overwrite").partitionBy("snapshot_date").parquet(join_path(base, "dim_driver_scd2")))


def write_snapshot_conflicts(df: DataFrame, base: str, snapshot_date: date) -> None:
    (df.withColumn("snapshot_date", F.lit(snapshot_date)).coalesce(1)
       .write.mode("overwrite").partitionBy("snapshot_date").parquet(join_path(base, "driver_snapshot_conflicts")))


def write_metrics(spark: SparkSession, base: str, processing_date: date, run_id: str, metrics: dict) -> None:
    payload = json.dumps(metrics, default=str, sort_keys=True)
    path = join_path(base, "_run_metrics", f"processing_date={processing_date}", f"run_id={run_id}")
    spark.createDataFrame([(payload,)], "metrics string").coalesce(1).write.mode("overwrite").text(path)
