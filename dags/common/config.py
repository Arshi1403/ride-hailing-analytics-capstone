"""Settings for the ride_hailing_daily DAG, read from environment variables (no network, no Airflow imports).

Set the variables on the Composer environment (see dags/config/environment.example.env). Secrets are never read here:
credentials come from Composer's service account and Airflow connections served by the Secret Manager backend.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Mapping

PROJECT_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
REGION_RE = re.compile(r"^[a-z]+-[a-z]+[0-9]+$")
BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,62}$")
SA_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$")
TZ_RE = re.compile(r"^(UTC|[A-Za-z]+(/[A-Za-z_+-]+){1,2})$")
CRON_RE = re.compile(r"^\S+ \S+ \S+ \S+ \S+$")
CONN_RE = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PREFIX_RE = re.compile(r"^[A-Za-z0-9_./-]{1,100}$")

REQUIRED = {
    "RH_LANDING_BUCKET": "bucket that holds the historical dump and reference files (name only, no gs://)",
    "RH_CURATED_BUCKET": "bucket for the Spark Parquet output (name only)",
    "RH_ARTIFACTS_BUCKET": "bucket with spark/code/run_job.py and ridespark.zip, also the Dataproc deps bucket (name only)",
    "RH_SPARK_SERVICE_ACCOUNT": "service account the Dataproc Serverless batch runs as",
}


class ConfigError(Exception):
    """Missing or invalid settings. The message lists every problem and how to fix it."""

    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("Invalid ride_hailing_daily configuration:\n- " + "\n- ".join(problems))


@dataclass(frozen=True)
class Settings:
    project_id: str
    region: str
    bq_location: str
    gcp_conn_id: str
    schedule_cron: str
    schedule_tz: str
    business_tz: str
    landing_bucket: str
    curated_bucket: str
    artifacts_bucket: str
    spark_service_account: str
    spark_subnet: str
    spark_runtime: str
    curated_prefix: str
    zones_uri: str
    driver_snapshot_uri: str
    historical_object_template: str
    historical_required: bool
    allow_missing_dimensions: bool
    lookback_days: int
    max_bytes_billed: int
    sql_dir: str
    alert_emails: tuple[str, ...]
    alert_conn_id: str
    retrain_weekday: int
    train_days: int
    top_n: int
    min_rides: int
    min_active_hours: int
    confidence_level: float

    @property
    def output_path(self) -> str:
        return f"gs://{self.curated_bucket}/{self.curated_prefix}"

    @property
    def spark_main_uri(self) -> str:
        return f"gs://{self.artifacts_bucket}/spark/code/run_job.py"

    @property
    def spark_zip_uri(self) -> str:
        return f"gs://{self.artifacts_bucket}/spark/code/ridespark.zip"

    @property
    def stream_input(self) -> str:
        return f"bq://{self.project_id}.raw_ds.rides_stream"

    @property
    def hist_input(self) -> str:
        return f"bq://{self.project_id}.raw_ds.rides_history"


def _flag(value: str, default: bool) -> bool:
    return default if value == "" else value.strip().lower() in ("1", "true", "yes")


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    env = os.environ if env is None else env

    def get(name: str, default: str = "") -> str:
        return (env.get(name) or "").strip() or default

    problems: list[str] = [f"{name} is not set: {why}" for name, why in REQUIRED.items() if not get(name)]

    def integer(name: str, default: int, low: int, high: int) -> int:
        raw = get(name, str(default))
        try:
            value = int(raw)
        except ValueError:
            problems.append(f"{name} must be an integer, got {raw!r}")
            return default
        if not low <= value <= high:
            problems.append(f"{name} must be between {low} and {high}, got {value}")
        return value

    project, region = get("GCP_PROJECT_ID", "labs-507309"), get("GCP_REGION", "asia-south1")
    landing, curated, artifacts = get("RH_LANDING_BUCKET"), get("RH_CURATED_BUCKET"), get("RH_ARTIFACTS_BUCKET")
    emails = tuple(e.strip() for e in get("RH_ALERT_EMAILS").split(",") if e.strip())
    try:
        confidence = float(get("RH_FORECAST_CONFIDENCE", "0.9"))
    except ValueError:
        confidence = -1.0
    landing_default = f"gs://{landing}/reference/taxi_zone_lookup.csv" if landing else ""

    settings = Settings(
        project_id=project,
        region=region,
        bq_location=get("RH_BQ_LOCATION", region),
        gcp_conn_id=get("RH_GCP_CONN_ID", "google_cloud_default"),
        schedule_cron=get("RH_SCHEDULE_CRON", "0 1 * * *"),
        schedule_tz=get("RH_SCHEDULE_TZ", "Asia/Kolkata"),
        business_tz=get("RH_BUSINESS_TZ", "UTC"),
        landing_bucket=landing,
        curated_bucket=curated,
        artifacts_bucket=artifacts,
        spark_service_account=get("RH_SPARK_SERVICE_ACCOUNT"),
        spark_subnet=get("RH_SPARK_SUBNET"),
        spark_runtime=get("RH_SPARK_RUNTIME", "2.2"),
        curated_prefix=get("RH_CURATED_PREFIX", "curated").strip("/"),
        zones_uri=get("RH_ZONES_URI", landing_default),
        driver_snapshot_uri=get("RH_DRIVER_SNAPSHOT_URI"),
        historical_object_template=get("RH_HISTORICAL_OBJECT_TEMPLATE", "historical/{ds}/yellow_tripdata_2018_sample.csv"),
        historical_required=_flag(get("RH_HISTORICAL_DUMP_REQUIRED"), True),
        allow_missing_dimensions=_flag(get("RH_ALLOW_MISSING_DIMENSIONS"), False),
        lookback_days=integer("RH_LOOKBACK_DAYS", 3, 0, 30),
        max_bytes_billed=integer("RH_MAX_BYTES_BILLED", 10_000_000_000, 1_000_000, 10_000_000_000_000),
        sql_dir=get("RH_SQL_DIR", "/home/airflow/gcs/data/sql"),
        alert_emails=emails,
        alert_conn_id=get("RH_ALERT_CONN_ID", "rh_alert_webhook"),
        retrain_weekday=integer("RH_RETRAIN_WEEKDAY", 0, 0, 6),
        train_days=integer("RH_FORECAST_TRAIN_DAYS", 56, 14, 365),
        top_n=integer("RH_FORECAST_TOP_N", 10, 1, 10),
        min_rides=integer("RH_FORECAST_MIN_RIDES", 500, 1, 10_000_000),
        min_active_hours=integer("RH_FORECAST_MIN_ACTIVE_HOURS", 168, 1, 8760),
        confidence_level=confidence,
    )

    for ok, message in (
        (PROJECT_RE.match(project), f"GCP_PROJECT_ID is not a valid project id: {project!r}"),
        (REGION_RE.match(region), f"GCP_REGION is not a valid region: {region!r}"),
        (CRON_RE.match(settings.schedule_cron), f"RH_SCHEDULE_CRON must have 5 fields, got {settings.schedule_cron!r}"),
        (TZ_RE.match(settings.schedule_tz), f"RH_SCHEDULE_TZ must be UTC or an IANA name, got {settings.schedule_tz!r}"),
        (TZ_RE.match(settings.business_tz), f"RH_BUSINESS_TZ must be UTC or an IANA name, got {settings.business_tz!r}"),
        (CONN_RE.match(settings.gcp_conn_id), "RH_GCP_CONN_ID has invalid characters"),
        (CONN_RE.match(settings.alert_conn_id), "RH_ALERT_CONN_ID has invalid characters"),
        (PREFIX_RE.match(settings.curated_prefix), "RH_CURATED_PREFIX has invalid characters"),
        (0 < confidence < 1, "RH_FORECAST_CONFIDENCE must be between 0 and 1"),
        (all(EMAIL_RE.match(e) for e in emails), "RH_ALERT_EMAILS must be a comma-separated list of e-mail addresses"),
        ("{ds}" in settings.historical_object_template or "/" in settings.historical_object_template,
         "RH_HISTORICAL_OBJECT_TEMPLATE must be an object path, optionally containing {ds}"),
    ):
        if not ok:
            problems.append(message)
    for name, value in (("RH_LANDING_BUCKET", landing), ("RH_CURATED_BUCKET", curated), ("RH_ARTIFACTS_BUCKET", artifacts)):
        if value and not BUCKET_RE.match(value):
            problems.append(f"{name} must be a bucket name without gs://, got {value!r}")
    if settings.spark_service_account and not SA_RE.match(settings.spark_service_account):
        problems.append("RH_SPARK_SERVICE_ACCOUNT must look like name@project.iam.gserviceaccount.com")
    if settings.zones_uri and not settings.zones_uri.startswith("gs://"):
        problems.append("RH_ZONES_URI must be a gs:// URI")
    if settings.driver_snapshot_uri and not settings.driver_snapshot_uri.startswith("gs://"):
        problems.append("RH_DRIVER_SNAPSHOT_URI must be a gs:// URI")
    if problems:
        raise ConfigError(problems)
    return settings
