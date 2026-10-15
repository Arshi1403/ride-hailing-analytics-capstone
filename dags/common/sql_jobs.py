"""BigQuery SQL jobs: which file(s) each task runs and which parameters each file needs.

The SQL lives in sql/ (owned by the warehouse agent) and is uploaded to the environment bucket under data/sql/.
Parameter names and types must match the `@name` parameters used in those files; tests/dags checks this against sql/.
"""
from __future__ import annotations

from dataclasses import dataclass

DAG_ID = "ride_hailing_daily"

# BigQuery parameter type of every @parameter used across sql/.
PARAM_TYPES = {
    "run_id": "STRING",
    "window_start": "DATE",
    "window_end": "DATE",
    "snapshot_date": "DATE",
    "business_tz": "STRING",
    "as_of_ts": "TIMESTAMP",
    "train_days": "INT64",
    "top_n": "INT64",
    "min_rides": "INT64",
    "min_active_hours": "INT64",
    "confidence_level": "FLOAT64",
}


@dataclass(frozen=True)
class SqlJob:
    task_id: str
    files: tuple[str, ...]
    params: tuple[str, ...]


JOBS = {
    "merge_fact": SqlJob("merge_fact", ("merge/merge_fact_rides.sql",), ("run_id", "window_start", "window_end")),
    "merge_rejected": SqlJob("merge_rejected", ("merge/merge_rejected_rides.sql",), ("run_id", "window_start", "window_end")),
    "merge_dim_scd2": SqlJob("merge_dim_scd2", ("merge/merge_dim_driver_scd2.sql",), ("run_id", "snapshot_date", "business_tz")),
    "refresh_mart_views": SqlJob(
        "refresh_mart_views",
        ("marts/v_daily_revenue.sql", "marts/v_zone_demand_hourly.sql", "marts/v_driver_leaderboard.sql",
         "marts/v_zone_demand_forecast.sql"),
        (),
    ),
    "build_training_series": SqlJob(
        "build_training_series", ("bqml/01_select_zones_and_series.sql",),
        ("as_of_ts", "train_days", "top_n", "min_rides", "min_active_hours", "run_id"),
    ),
    "retrain_bqml": SqlJob("retrain_bqml", ("bqml/02_train_arima_plus.sql",), ()),
    "forecast_168h": SqlJob("forecast_168h", ("bqml/03_forecast_168h.sql",), ("as_of_ts", "run_id", "confidence_level")),
}

# Checks run from Python so that results can be summarised and stored: (file, parameters it needs).
DQ_FILES = (
    "data_quality/dq_row_counts.sql",
    "data_quality/dq_null_thresholds.sql",
    "data_quality/dq_duplicates.sql",
    "data_quality/dq_negative_fares.sql",
    "data_quality/dq_long_trips.sql",
    "data_quality/dq_missing_dimensions.sql",
)
FORECAST_VALIDATION_FILE = "bqml/04_validate_forecast.sql"


def param_templates(settings) -> dict[str, str]:
    """Jinja (or literal) value of each parameter. All macros are defined in common/windows.py."""
    return {
        "run_id": "{{ rh_run_id(ds, ts_nodash) }}",
        "window_start": "{{ rh_window_start(ds, params) }}",
        "window_end": "{{ ds }}",
        "snapshot_date": "{{ ds }}",
        "business_tz": settings.business_tz,
        "as_of_ts": "{{ rh_as_of_ts(ds) }}",
        "train_days": str(settings.train_days),
        "top_n": str(settings.top_n),
        "min_rides": str(settings.min_rides),
        "min_active_hours": str(settings.min_active_hours),
        "confidence_level": str(settings.confidence_level),
    }


def query_configuration(job: SqlJob, settings) -> dict:
    """BigQuery job configuration (REST form) for BigQueryInsertJobOperator. The SQL text is included by Jinja at render time."""
    templates = param_templates(settings)
    query: dict = {
        "query": "\n".join("{% include '" + file + "' %}" for file in job.files),
        "useLegacySql": False,
        "maximumBytesBilled": str(settings.max_bytes_billed),
    }
    if job.params:
        query["parameterMode"] = "NAMED"
        query["queryParameters"] = [
            {"name": name, "parameterType": {"type": PARAM_TYPES[name]}, "parameterValue": {"value": templates[name]}}
            for name in job.params
        ]
    return {"query": query, "labels": {"pipeline": "ride_hailing", "dag": DAG_ID, "task": job.task_id.replace("_", "-")}}
