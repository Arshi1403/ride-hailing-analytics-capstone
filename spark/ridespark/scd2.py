"""SCD Type 2 driver history built from a daily snapshot.

Dates, not timestamps: a version is valid for effective_from <= date < effective_to; the current version
ends at 9999-12-31. effective_from of a driver's first version is the first snapshot date that contained
the driver (the real start date is not known, so it is not invented).
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .schemas import DRIVER_SCD2_SCHEMA, DRIVER_SNAPSHOT_SCHEMA, DRIVER_TRACKED_COLUMNS, conform

END_OF_TIME = date(9999, 12, 31)
_NULL_MARK = "\u0000NULL"


class Scd2Error(ValueError):
    pass


def record_hash() -> F.Column:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit(_NULL_MARK)) for c in DRIVER_TRACKED_COLUMNS]
    return F.sha2(F.concat_ws("\u001f", *parts), 256)


def prepare_snapshot(snapshot_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (clean rows with record_hash, conflicts).

    Conflicts are snapshot rows that cannot be trusted: null driver_id, or the same driver_id listed twice
    with different attributes. Conflicting drivers are left unchanged in the history. Exact duplicate rows collapse."""
    snap = conform(snapshot_df, DRIVER_SNAPSHOT_SCHEMA, "driver snapshot")
    trimmed = F.trim(F.col("driver_id"))
    snap = snap.withColumn("driver_id", F.when(trimmed == "", None).otherwise(trimmed))
    snap = snap.withColumn("record_hash", record_hash())

    null_ids = snap.where(F.col("driver_id").isNull()).withColumn("conflict_reason", F.lit("NULL_DRIVER_ID"))
    keyed = snap.where(F.col("driver_id").isNotNull()).dropDuplicates()
    variants = keyed.groupBy("driver_id").agg(F.countDistinct("record_hash").alias("_n"))
    conflicting_ids = variants.where(F.col("_n") > 1).select("driver_id")

    conflicts = (
        keyed.join(conflicting_ids, "driver_id", "left_semi")
        .withColumn("conflict_reason", F.lit("CONFLICTING_SNAPSHOT_ROWS"))
        .unionByName(null_ids)
    )
    clean = keyed.join(conflicting_ids, "driver_id", "left_anti")
    return clean, conflicts


def empty_history(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(spark.sparkContext.emptyRDD(), DRIVER_SCD2_SCHEMA)


def validate_scd2(history: DataFrame) -> None:
    """Raise Scd2Error unless: exactly one current row per driver, from < to, versions contiguous."""
    problems = []
    per_driver = history.groupBy("driver_id").agg(F.sum(F.col("is_current").cast("int")).alias("n_current"))
    bad_current = per_driver.where(F.col("n_current") != 1).count()
    if bad_current:
        problems.append(f"{bad_current} driver(s) do not have exactly one current record")
    if history.where(F.col("effective_from") >= F.col("effective_to")).count():
        problems.append("some versions have effective_from >= effective_to")
    order = Window.partitionBy("driver_id").orderBy("effective_from")
    nxt = history.withColumn("_next_from", F.lead("effective_from").over(order))
    gaps = nxt.where(F.col("_next_from").isNotNull() & (F.col("effective_to") != F.col("_next_from"))).count()
    if gaps:
        problems.append(f"{gaps} version(s) overlap or leave a gap before the next version")
    wrong_flag = history.where(F.col("is_current") != (F.col("effective_to") == F.lit(END_OF_TIME))).count()
    if wrong_flag:
        problems.append(f"{wrong_flag} version(s) have is_current inconsistent with effective_to")
    if problems:
        raise Scd2Error("SCD2 integrity check failed: " + "; ".join(problems))


def _rewind(history: DataFrame, snapshot_date: date) -> DataFrame:
    """Undo a previous application of the same snapshot_date so that reruns are exact repeats."""
    d = F.lit(snapshot_date)
    kept = history.where(F.col("effective_from") != d)
    reopened = F.col("effective_to") == d
    return kept.select(
        "driver_id", "vehicle_type", "home_zone", "status", "record_hash", "effective_from",
        F.when(reopened, F.lit(END_OF_TIME)).otherwise(F.col("effective_to")).alias("effective_to"),
        (F.col("is_current") | reopened).alias("is_current"),
    )


def apply_snapshot(history: DataFrame, snapshot_df: DataFrame, snapshot_date: date) -> DataFrame:
    """Return the full history after applying the snapshot taken on `snapshot_date`.

    New driver -> new current version. Changed tracked attribute -> close current, open a new one.
    Unchanged -> nothing. Driver absent from the snapshot -> left as is (absence is not a deletion;
    send status such as INACTIVE to end activity). Re-applying the same date gives the same history.
    Applying a date older than the history's latest version raises Scd2Error."""
    latest = history.agg(F.max("effective_from")).first()[0]
    if latest is not None and latest > snapshot_date:
        raise Scd2Error(
            f"history already contains versions from {latest}, which is after snapshot date {snapshot_date}; "
            "only the latest snapshot date can be rerun"
        )
    history = _rewind(history, snapshot_date)
    snap, _ = prepare_snapshot(snapshot_df)

    current = history.where(F.col("is_current")).select("driver_id", F.col("record_hash").alias("_cur_hash"))
    changed_ids = (
        snap.join(current, "driver_id", "inner").where(F.col("record_hash") != F.col("_cur_hash")).select("driver_id")
    )
    new_ids = snap.join(current.select("driver_id"), "driver_id", "left_anti").select("driver_id")

    flagged = history.join(changed_ids.withColumn("_changed", F.lit(True)), "driver_id", "left")
    closing = F.col("is_current") & F.col("_changed").isNotNull()
    updated = flagged.select(
        "driver_id", "vehicle_type", "home_zone", "status", "record_hash", "effective_from",
        F.when(closing, F.lit(snapshot_date)).otherwise(F.col("effective_to")).alias("effective_to"),
        F.when(closing, F.lit(False)).otherwise(F.col("is_current")).alias("is_current"),
    )
    opened = (
        snap.join(changed_ids.unionByName(new_ids), "driver_id", "left_semi")
        .select(
            "driver_id", "vehicle_type", "home_zone", "status", "record_hash",
            F.lit(snapshot_date).alias("effective_from"),
            F.lit(END_OF_TIME).alias("effective_to"),
            F.lit(True).alias("is_current"),
        )
    )
    result = updated.unionByName(opened)
    return conform(result, DRIVER_SCD2_SCHEMA, "scd2 result")
