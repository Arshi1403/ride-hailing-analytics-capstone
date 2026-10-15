"""Alerting: compact, redacted messages sent through logs, e-mail (task default_args) and an optional webhook connection.

Notification approach
- Task failures, DAG failures, SLA misses and data-quality failures all call send_alert().
- Channels: (1) the Airflow task log (always); (2) e-mail via Airflow's own e-mail settings when RH_ALERT_EMAILS is set;
  (3) a webhook (Slack, Google Chat or similar) when the Airflow connection RH_ALERT_CONN_ID exists. The webhook URL is
  never in code: it lives in a Secret Manager secret served as an Airflow connection.
- Messages carry DAG, task, run id, try number, error class and a trimmed message. They never carry connection values,
  XCom payloads or environment variables, and everything passes through redact().
"""
from __future__ import annotations

import json
import logging
import re

log = logging.getLogger("airflow.task")

MAX_MESSAGE = 600
_PATTERNS = (
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S), "[private key]"),
    (re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}"), r"\1 [redacted]"),
    (re.compile(r"\bya29\.[A-Za-z0-9._-]+"), "[access token]"),
    (re.compile(r"\bAIza[0-9A-Za-z_-]{20,}"), "[api key]"),
    (re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"), "[slack token]"),
    (re.compile(r"(?i)\b(password|passwd|secret|token|api[_-]?key|access[_-]?key|authorization)\s*[=:]\s*\S+"), r"\1=[redacted]"),
    (re.compile(r"(?i)(https?://)[^\s/@]+:[^\s/@]+@"), r"\1[redacted]@"),
    (re.compile(r"(?i)(https://hooks\.[^\s]+)"), "[webhook url]"),
)


def redact(text: str, limit: int = MAX_MESSAGE) -> str:
    text = "" if text is None else str(text)
    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def failure_message(context: dict) -> str:
    ti = context.get("task_instance") or context.get("ti")
    exc = context.get("exception")
    parts = [
        f"FAILED {getattr(ti, 'dag_id', context.get('dag_id', '?'))}.{getattr(ti, 'task_id', '?')}",
        f"run={context.get('run_id', '?')}",
        f"try={getattr(ti, 'try_number', '?')}",
    ]
    if exc is not None:
        parts.append(f"error={type(exc).__name__}: {redact(str(exc), 300)}")
    log_url = getattr(ti, "log_url", None)
    if log_url:
        parts.append(f"log={redact(log_url, 300)}")
    return redact(" | ".join(parts))


def success_message(dag_id: str, run_id: str, dq_summary: dict | None, forecast_summary: dict | None = None) -> str:
    parts = [f"SUCCESS {dag_id} run={run_id}"]
    if dq_summary:
        parts.append(f"dq checks={dq_summary.get('checks')} warn={dq_summary.get('warned')} fail={dq_summary.get('failed')}")
    if forecast_summary:
        parts.append(f"forecast checks={forecast_summary.get('checks')} fail={forecast_summary.get('failed')}")
    return redact(" | ".join(parts))


def send_alert(message: str, conn_id: str, level: int = logging.ERROR) -> bool:
    """Log the message and post it to the webhook connection when one exists. Never raises: alerting must not hide the real failure."""
    message = redact(message)
    log.log(level, "[rh][alert] %s", message)
    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(conn_id)
    except Exception:  # noqa: BLE001 - connection missing or backend unavailable
        log.warning("[rh] alert connection %r is not configured; the alert was only logged", conn_id)
        return False
    try:
        import requests

        scheme = conn.schema or "https"
        url = f"{scheme}://{conn.host}/{(conn.password or '').lstrip('/')}".rstrip("/")
        response = requests.post(url, data=json.dumps({"text": message}), headers={"Content-Type": "application/json"}, timeout=10)
        response.raise_for_status()
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("[rh] could not post the alert to %r: %s", conn_id, type(exc).__name__)
        return False


def make_callbacks(conn_id: str):
    """Callbacks bound to the alert connection id (no secrets are captured, only the id)."""

    def on_task_failure(context):
        send_alert(failure_message(context), conn_id)

    def on_dag_failure(context):
        send_alert("DAG RUN FAILED " + failure_message(context), conn_id)

    def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
        names = ", ".join(sorted({str(getattr(s, "task_id", "?")) for s in slas}))
        send_alert(f"SLA MISSED {getattr(dag, 'dag_id', '?')} (4h end-to-end target) tasks: {names}", conn_id)

    return on_task_failure, on_dag_failure, on_sla_miss
