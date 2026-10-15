"""Python callables for the DAG. They translate pure-logic outcomes into Airflow results and exceptions.
XCom carries only the compact dq summary (counts and check ids)."""
from __future__ import annotations

import logging
import os

from . import alerts, dq
from .config import load_settings
from .gcp import BigQueryAdapter, GcsAdapter
from .staging import SPECS, StagingError, StagingSkip, run_staging_load
from .windows import run_id as make_run_id
from .windows import window_start

log = logging.getLogger("airflow.task")


def _settings():
    return load_settings()


def _sql_reader(settings):
    base = os.path.realpath(settings.sql_dir)

    def read(relative: str) -> str:
        path = os.path.realpath(os.path.join(base, relative))
        if not path.startswith(base + os.sep):
            raise dq.DqRunError(f"SQL path escapes the SQL directory: {relative}")
        if not os.path.isfile(path):
            raise dq.DqRunError(
                f"SQL file not found: {path}. Upload sql/ to the environment bucket under data/sql/ (RH_SQL_DIR={settings.sql_dir})."
            )
        with open(path, encoding="utf-8") as handle:
            return handle.read()

    return read


def _values(settings, context) -> dict[str, str]:
    from .windows import as_of_ts

    ds = context["ds"]
    params = context.get("params") or {}
    return {
        "run_id": make_run_id(ds, context["ts_nodash"]),
        "window_start": window_start(ds, params, settings.lookback_days),
        "window_end": ds,
        "snapshot_date": ds,
        "business_tz": settings.business_tz,
        "as_of_ts": as_of_ts(ds, settings.business_tz),
        "top_n": str(settings.top_n),
    }


def load_staging(task_key: str, **context):
    from airflow.exceptions import AirflowFailException, AirflowSkipException

    settings = _settings()
    try:
        return run_staging_load(settings, SPECS[task_key], context["ds"], context.get("params") or {},
                                GcsAdapter(settings), BigQueryAdapter(settings))
    except StagingSkip as skip:
        raise AirflowSkipException(str(skip)) from None
    except StagingError as err:
        raise AirflowFailException(str(err)) from None


def dq_checks(**context) -> dict:
    """Run all data-quality SQL, store the rows in ops_ds.dq_results, return a compact summary for XCom."""
    from airflow.exceptions import AirflowFailException

    settings = _settings()
    values = _values(settings, context)
    try:
        rows = dq.run_checks(BigQueryAdapter(settings), _sql_reader(settings), values, settings.max_bytes_billed)
    except dq.DqRunError as err:
        raise AirflowFailException(str(err)) from None  # a broken check is not retried: it will not heal by itself
    summary = dq.summarize(rows, values["run_id"])
    log.info("[rh] data-quality summary: %s", summary)
    return summary


def branch_on_dq(ti, **context) -> str:
    summary = ti.xcom_pull(task_ids="dq_checks")
    branch = dq.decide_dq_branch(summary)
    log.info("[rh] dq branch -> %s (summary: %s)", branch, summary)
    return branch


def alert_dq_failure(ti, **context):
    from airflow.exceptions import AirflowFailException

    settings = _settings()
    summary = ti.xcom_pull(task_ids="dq_checks") or {"run_id": make_run_id(context["ds"], context["ts_nodash"]),
                                                    "failed": "unknown", "failed_ids": [], "warned": "?"}
    message = dq.dq_failure_message(summary)
    alerts.send_alert("DATA QUALITY FAILED | " + message, settings.alert_conn_id)
    raise AirflowFailException(message)


def branch_weekly_retrain(**context) -> str:
    from airflow.models import Variable

    settings = _settings()
    enabled = str(Variable.get("rh_enable_bqml_retrain", default_var="false")).strip().lower() == "true"
    branch, reason = dq.decide_retrain(context["logical_date"], context.get("params"), settings, enabled)
    log.info("[rh] retrain branch -> %s: %s", branch, reason)
    return branch


def validate_forecast(**context) -> dict:
    """Run the forecast validation SQL; a FAIL stops the run before success is announced."""
    from airflow.exceptions import AirflowFailException

    settings = _settings()
    values = _values(settings, context)
    try:
        rows = dq.run_checks(BigQueryAdapter(settings), _sql_reader(settings), values, settings.max_bytes_billed,
                             files=(dq.FORECAST_VALIDATION_FILE,))
    except dq.DqRunError as err:
        raise AirflowFailException(str(err)) from None
    summary = dq.summarize(rows, values["run_id"])
    if summary["failed"]:
        message = "Forecast validation failed: " + ", ".join(summary["failed_ids"])
        alerts.send_alert(message, settings.alert_conn_id)
        raise AirflowFailException(message)
    return summary


def notify_success(ti, **context):
    from airflow.exceptions import AirflowFailException

    settings = _settings()
    summary = ti.xcom_pull(task_ids="dq_checks")
    if dq.decide_dq_branch(summary) != dq.BRANCH_PASS:
        raise AirflowFailException("notify_success refuses to run: the data-quality summary is missing or has failures")
    forecast = ti.xcom_pull(task_ids="validate_forecast")
    alerts.send_alert(alerts.success_message(context["dag"].dag_id, make_run_id(context["ds"], context["ts_nodash"]),
                                             summary, forecast), settings.alert_conn_id, level=logging.INFO)
    log.info("[rh] run %s finished successfully", make_run_id(context["ds"], context["ts_nodash"]))
