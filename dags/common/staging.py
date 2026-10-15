"""Load one run's Spark Parquet output into the stg_ds tables (the merges require staging to hold exactly this run).

Why Python and not one operator per table: only the pickup-date (or event-date) folders inside the run window may be
loaded, because the Spark output folder also keeps older partitions. The folder list changes per run, so it is built at
run time. Everything is idempotent: each load replaces the staging table.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from .windows import window_dates, window_start

log = logging.getLogger("airflow.task")


class StagingError(Exception):
    """Fatal: the run cannot continue (message says what to check)."""


class StagingSkip(Exception):
    """The step does not apply to this run (translated to an Airflow skip)."""


@dataclass(frozen=True)
class StagingSpec:
    task_id: str
    folder: str
    partition_key: str
    table: str
    required: bool


SPECS = {
    "load_staging_rides": StagingSpec("load_staging_rides", "rides", "pickup_date", "stg_ds.fact_rides_stg", True),
    "load_staging_rejected": StagingSpec("load_staging_rejected", "rejected", "event_date", "stg_ds.rejected_rides_stg", False),
    "load_staging_dim": StagingSpec("load_staging_dim", "dim_driver_scd2", "snapshot_date", "stg_ds.dim_driver_scd2_stg", False),
}


def partition_dates(spec: StagingSpec, settings, ds: str, params) -> list[str]:
    if spec.folder == "dim_driver_scd2":
        return [ds]  # only the snapshot of this run; older snapshots stay in GCS
    return window_dates(window_start(ds, params, settings.lookback_days), ds)


def run_staging_load(settings, spec: StagingSpec, ds: str, params, gcs, bq) -> dict:
    if spec.folder == "dim_driver_scd2" and not settings.driver_snapshot_uri:
        raise StagingSkip("RH_DRIVER_SNAPSHOT_URI is not set: no new driver snapshot, the SCD2 dimension is left unchanged")

    base = f"{settings.curated_prefix}/{spec.folder}"
    found, missing = [], []
    for day in partition_dates(spec, settings, ds, params):
        prefix = f"{base}/{spec.partition_key}={day}/"
        (found if gcs.prefix_has_objects(settings.curated_bucket, prefix) else missing).append((day, prefix))

    if not found:
        if spec.required or spec.folder == "dim_driver_scd2":
            raise StagingError(
                f"No Spark output under gs://{settings.curated_bucket}/{base}/ for {spec.partition_key} "
                f"{[d for d, _ in missing][0]}..{[d for d, _ in missing][-1]}. Check that run_curated_spark succeeded "
                "and that --output-path matches RH_CURATED_BUCKET/RH_CURATED_PREFIX."
            )
        bq.empty_table(spec.table)
        log.info("[rh] %s: no Spark output for the window, %s emptied (nothing to merge)", spec.task_id, spec.table)
        return {"table": spec.table, "partitions_loaded": 0, "partitions_missing": len(missing), "rows": 0}

    uris = [f"gs://{settings.curated_bucket}/{prefix}*" for _, prefix in found]
    rows = bq.load_parquet(uris, spec.table, hive_prefix=f"gs://{settings.curated_bucket}/{base}/")
    log.info("[rh] %s: loaded %d partition(s), %d row(s) into %s (%d window date(s) without output)",
             spec.task_id, len(found), rows, spec.table, len(missing))
    return {"table": spec.table, "partitions_loaded": len(found), "partitions_missing": len(missing), "rows": rows}
