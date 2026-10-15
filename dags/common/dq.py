"""Data-quality execution, compact XCom summary, and branch decisions. Pure Python: BigQuery access is injected."""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Callable

from .sql_jobs import DQ_FILES, FORECAST_VALIDATION_FILE, PARAM_TYPES

log = logging.getLogger("airflow.task")

DQ_TABLE = "ops_ds.dq_results"
MAX_IDS_IN_SUMMARY = 20
BRANCH_PASS = "refresh_mart_views"
BRANCH_FAIL = "alert_dq_failure"
BRANCH_RETRAIN = "build_training_series"
BRANCH_SKIP_RETRAIN = "skip_retrain"


class DqRunError(Exception):
    """Permanent problem (missing SQL file or parameter). Retrying cannot fix it."""


class DqQueryError(Exception):
    """A BigQuery query failed. May be transient, so Airflow retries the task."""


def params_in_sql(sql: str) -> list[str]:
    code = re.sub(r"--[^\n]*", "", sql)
    code = re.sub(r"'(?:[^'\\]|\\.)*'", "''", code)
    return sorted(set(re.findall(r"(?<![\w@])@(\w+)", code)))


def typed_value(kind: str, text: str):
    if kind == "STRING":
        return text
    if kind == "DATE":
        return date.fromisoformat(text)
    if kind == "TIMESTAMP":
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    if kind == "INT64":
        return int(text)
    if kind == "FLOAT64":
        return float(text)
    raise ValueError(f"unsupported parameter type {kind}")


def bind_params(sql: str, values: dict[str, str], file: str) -> list[tuple[str, str, object]]:
    bound = []
    for name in params_in_sql(sql):
        if name not in values:
            raise DqRunError(f"{file} needs parameter @{name}, which the DAG does not provide. Add it to the DAG or fix the SQL.")
        bound.append((name, PARAM_TYPES[name], typed_value(PARAM_TYPES[name], values[name])))
    return bound


def run_sql_file(bq, read_sql: Callable[[str], str], file: str, values: dict[str, str], max_bytes: int) -> list[dict]:
    sql = read_sql(file)
    try:
        return bq.run_query(sql, bind_params(sql, values, file), max_bytes)
    except DqRunError:
        raise
    except Exception as exc:  # noqa: BLE001 - add the file name so the failing check is obvious
        raise DqQueryError(f"{file} failed: {type(exc).__name__}: {str(exc)[:300]}") from exc


def run_checks(bq, read_sql, values: dict[str, str], max_bytes: int, files=DQ_FILES, persist: bool = True) -> list[dict]:
    rows: list[dict] = []
    for file in files:
        result = run_sql_file(bq, read_sql, file, values, max_bytes)
        log.info("[rh] %s returned %d check row(s)", file, len(result))
        rows.extend(result)
    if persist and rows:
        bq.append_rows(DQ_TABLE, rows)
    return rows


def summarize(rows: list[dict], run_id: str) -> dict:
    """Compact result for XCom: counts and check ids only (no details, no values, nothing sensitive)."""
    failed = sorted(r["check_id"] for r in rows if r.get("status") == "FAIL")
    warned = sorted(r["check_id"] for r in rows if r.get("status") == "WARN")
    return {
        "run_id": run_id,
        "checks": len(rows),
        "passed": sum(1 for r in rows if r.get("status") == "PASS"),
        "warned": len(warned),
        "failed": len(failed),
        "failed_ids": failed[:MAX_IDS_IN_SUMMARY],
        "warned_ids": warned[:MAX_IDS_IN_SUMMARY],
    }


def decide_dq_branch(summary: dict | None) -> str:
    """Continue only when the summary exists and has no FAIL. A missing or malformed summary is treated as a failure."""
    if not isinstance(summary, dict) or "failed" not in summary or summary.get("checks", 0) <= 0:
        return BRANCH_FAIL
    return BRANCH_FAIL if summary["failed"] else BRANCH_PASS


def decide_retrain(logical_date, params, settings, enabled: bool) -> tuple[str, str]:
    """(branch task id, reason). Weekly on settings.retrain_weekday, only when enabled; backfills never retrain unless forced."""
    from .windows import backfill_days

    forced = str(params.get("force_retrain", False) if params else False).lower() in ("true", "1")
    if not enabled:
        return BRANCH_SKIP_RETRAIN, "retraining is disabled (Airflow Variable rh_enable_bqml_retrain is not 'true')"
    if forced:
        return BRANCH_RETRAIN, "force_retrain parameter set"
    if backfill_days(params):
        return BRANCH_SKIP_RETRAIN, "backfill run: the model is only retrained when force_retrain is set"
    if logical_date.weekday() == settings.retrain_weekday:
        return BRANCH_RETRAIN, f"weekly retrain day (weekday {settings.retrain_weekday})"
    return BRANCH_SKIP_RETRAIN, f"not the weekly retrain day (weekday {settings.retrain_weekday})"


def dq_failure_message(summary: dict) -> str:
    ids = ", ".join(summary.get("failed_ids", [])) or "unknown"
    return (
        f"Data-quality checks failed for run {summary.get('run_id')}: {summary.get('failed')} FAIL, "
        f"{summary.get('warned')} WARN. Failed checks: {ids}. Details: SELECT * FROM ops_ds.dq_results "
        f"WHERE run_id = '{summary.get('run_id')}' AND status = 'FAIL'. Marts and forecast were not refreshed."
    )
