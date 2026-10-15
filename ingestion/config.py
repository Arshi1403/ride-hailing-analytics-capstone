"""Ingestion configuration from environment variables, validated before any cloud call."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping

PROJECT_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
REGION_RE = re.compile(r"^[a-z]+-[a-z]+[0-9]+$")
DATASET_RE = re.compile(r"^[A-Za-z0-9_]{1,1024}$")
TABLE_RE = re.compile(r"^[A-Za-z0-9_]{1,1024}$")
SUBSCRIPTION_RE = re.compile(r"^(?!goog)[A-Za-z][A-Za-z0-9._~+%-]{2,254}$")
GCS_URI_RE = re.compile(r"^gs://[a-z0-9][a-z0-9._-]{1,220}(/.*)?$")
SERVICE_ACCOUNT_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$")
JOB_NAME_RE = re.compile(r"^[a-z]([-a-z0-9]{0,38}[a-z0-9])?$")


class ConfigError(Exception):
    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("; ".join(problems))


@dataclass(frozen=True)
class IngestionConfig:
    project_id: str = "labs-507309"
    region: str = "asia-south1"
    raw_dataset: str = "raw_ds"
    stream_table: str = "rides_stream"
    hist_table: str = "rides_history"
    subscription: str = "rides-events-sub"
    dlq_table: str = ""
    template_path: str = ""
    udf_gcs_path: str = ""
    udf_function_name: str = "process"
    temp_location: str = ""
    service_account_email: str = ""
    job_name: str = "rides-pubsub-to-bq"
    max_workers: int = 2
    hist_year: int = 2018
    hist_file_pattern: str = r".*2018.*\.csv(\.gz)?$"

    @property
    def stream_table_spec(self) -> str:
        """Dataflow template format: project:dataset.table"""
        return f"{self.project_id}:{self.raw_dataset}.{self.stream_table}"

    @property
    def dlq_table_spec(self) -> str:
        return f"{self.project_id}:{self.raw_dataset}.{self.dlq_table}"

    def fq_table(self, table: str) -> str:
        """Standard SQL format: project.dataset.table"""
        return f"{self.project_id}.{self.raw_dataset}.{table}"


def load_config(env: Mapping[str, str]) -> IngestionConfig:
    """Build config from env vars (defaults are proposals, not proof that resources exist)."""
    def get(name: str, default: str) -> str:
        value = env.get(name, "")
        return value if value else default

    problems: list[str] = []
    try:
        max_workers = int(get("DATAFLOW_MAX_WORKERS", "2"))
        hist_year = int(get("HIST_YEAR", "2018"))
    except ValueError:
        raise ConfigError(["DATAFLOW_MAX_WORKERS and HIST_YEAR must be integers"]) from None

    region = get("GCP_REGION", "asia-south1")
    stream_table = get("BQ_STREAM_TABLE", "rides_stream")
    cfg = IngestionConfig(
        project_id=get("GCP_PROJECT_ID", "labs-507309"),
        region=region,
        raw_dataset=get("BQ_RAW_DATASET", "raw_ds"),
        stream_table=stream_table,
        hist_table=get("BQ_HIST_TABLE", "rides_history"),
        subscription=get("PUBSUB_SUBSCRIPTION", "rides-events-sub"),
        dlq_table=get("BQ_DLQ_TABLE", f"{stream_table}_error_records"),
        template_path=get("DATAFLOW_TEMPLATE_PATH", f"gs://dataflow-templates-{region}/latest/PubSub_Subscription_to_BigQuery"),
        udf_gcs_path=get("DATAFLOW_UDF_GCS_PATH", ""),
        udf_function_name=get("DATAFLOW_UDF_FUNCTION", "process"),
        temp_location=get("DATAFLOW_TEMP_LOCATION", ""),
        service_account_email=get("DATAFLOW_SERVICE_ACCOUNT", ""),
        job_name=get("DATAFLOW_JOB_NAME", "rides-pubsub-to-bq"),
        max_workers=max_workers,
        hist_year=hist_year,
        hist_file_pattern=get("HIST_FILE_PATTERN", r".*2018.*\.csv(\.gz)?$"),
    )

    checks = [
        (PROJECT_RE, cfg.project_id, "GCP_PROJECT_ID"),
        (REGION_RE, cfg.region, "GCP_REGION"),
        (DATASET_RE, cfg.raw_dataset, "BQ_RAW_DATASET"),
        (TABLE_RE, cfg.stream_table, "BQ_STREAM_TABLE"),
        (TABLE_RE, cfg.hist_table, "BQ_HIST_TABLE"),
        (TABLE_RE, cfg.dlq_table, "BQ_DLQ_TABLE"),
        (SUBSCRIPTION_RE, cfg.subscription, "PUBSUB_SUBSCRIPTION (short name, not a full path)"),
        (JOB_NAME_RE, cfg.job_name, "DATAFLOW_JOB_NAME"),
    ]
    for pattern, value, name in checks:
        if not pattern.match(value):
            problems.append(f"{name} has an invalid value: {value!r}")
    if cfg.stream_table == cfg.hist_table:
        problems.append("BQ_STREAM_TABLE and BQ_HIST_TABLE must differ")
    if not 1 <= cfg.max_workers <= 10:
        problems.append("DATAFLOW_MAX_WORKERS must be between 1 and 10")
    if cfg.hist_year != 2018:
        problems.append("HIST_YEAR must be 2018: the capstone requires a subset of NYC Taxi 2018 data")
    try:
        re.compile(cfg.hist_file_pattern)
    except re.error as exc:
        problems.append(f"HIST_FILE_PATTERN is not a valid regex: {exc}")
    if problems:
        raise ConfigError(problems)
    return cfg


def require_for_dataflow(cfg: IngestionConfig) -> None:
    problems = []
    if not GCS_URI_RE.match(cfg.template_path):
        problems.append(f"DATAFLOW_TEMPLATE_PATH must be a gs:// path, got {cfg.template_path!r}")
    if not GCS_URI_RE.match(cfg.udf_gcs_path) or not cfg.udf_gcs_path.endswith(".js"):
        problems.append("DATAFLOW_UDF_GCS_PATH is required: gs://<bucket>/<path>/ride_event_udf.js")
    if not GCS_URI_RE.match(cfg.temp_location):
        problems.append("DATAFLOW_TEMP_LOCATION is required: gs://<bucket>/<path>")
    if not SERVICE_ACCOUNT_RE.match(cfg.service_account_email):
        problems.append("DATAFLOW_SERVICE_ACCOUNT is required: <name>@<project>.iam.gserviceaccount.com")
    if problems:
        raise ConfigError(problems)
