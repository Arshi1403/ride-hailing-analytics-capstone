"""Print the `gcloud dataproc batches submit pyspark` command. It never submits anything:
review the command, get approval, then run it yourself."""
from __future__ import annotations

import argparse
import os
import re
import shlex
import sys
import uuid
from datetime import date

PROJECT_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
REGION_RE = re.compile(r"^[a-z]+-[a-z]+[0-9]+$")
GCS_RE = re.compile(r"^gs://[a-z0-9][a-z0-9._-]{1,220}(/.*)?$")
SA_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$")
BATCH_RE = re.compile(r"^[a-z]([-a-z0-9]{2,60}[a-z0-9])$")


class SubmitConfigError(Exception):
    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("; ".join(problems))


def build_command(env: dict[str, str], processing_date: str, extra_args: list[str] | None = None) -> list[str]:
    def get(name: str, default: str = "") -> str:
        return env.get(name) or default

    project = get("GCP_PROJECT_ID", "labs-507309")
    region = get("GCP_REGION", "asia-south1")
    raw_dataset = get("BQ_RAW_DATASET", "raw_ds")
    batch = get("DATAPROC_BATCH_ID", f"rides-curation-{processing_date.replace('-', '')}-{uuid.uuid4().hex[:6]}")
    main_uri, zip_uri = get("SPARK_MAIN_URI"), get("SPARK_PY_FILES_URI")
    deps_bucket, output = get("DATAPROC_DEPS_BUCKET"), get("SPARK_OUTPUT_PATH")
    service_account, subnet = get("DATAPROC_SERVICE_ACCOUNT"), get("DATAPROC_SUBNET")
    runtime = get("SPARK_RUNTIME_VERSION", "2.2")
    stream = get("SPARK_STREAM_INPUT", f"bq://{project}.{raw_dataset}.{get('BQ_STREAM_TABLE', 'rides_stream')}")
    hist = get("SPARK_HIST_INPUT", f"bq://{project}.{raw_dataset}.{get('BQ_HIST_TABLE', 'rides_history')}")

    problems = []
    try:
        date.fromisoformat(processing_date)
    except ValueError:
        problems.append("processing date must be YYYY-MM-DD")
    for ok, message in (
        (PROJECT_RE.match(project), "GCP_PROJECT_ID is invalid"),
        (REGION_RE.match(region), "GCP_REGION is invalid"),
        (BATCH_RE.match(batch), "DATAPROC_BATCH_ID must be lowercase letters, digits, hyphens (4-62 chars)"),
        (GCS_RE.match(main_uri) and main_uri.endswith(".py"), "SPARK_MAIN_URI is required: gs://<bucket>/spark/code/run_job.py"),
        (GCS_RE.match(zip_uri) and zip_uri.endswith(".zip"), "SPARK_PY_FILES_URI is required: gs://<bucket>/spark/code/ridespark.zip"),
        (GCS_RE.match(deps_bucket) and "/" not in deps_bucket[5:], "DATAPROC_DEPS_BUCKET is required: gs://<bucket>"),
        (GCS_RE.match(output), "SPARK_OUTPUT_PATH is required: gs://<bucket>/<prefix>"),
        (SA_RE.match(service_account), "DATAPROC_SERVICE_ACCOUNT is required: <name>@<project>.iam.gserviceaccount.com"),
    ):
        if not ok:
            problems.append(message)
    if problems:
        raise SubmitConfigError(problems)

    command = [
        "gcloud", "dataproc", "batches", "submit", "pyspark", main_uri,
        f"--project={project}", f"--region={region}", f"--batch={batch}", f"--version={runtime}",
        f"--deps-bucket={deps_bucket}", f"--py-files={zip_uri}", f"--service-account={service_account}",
        "--properties=spark.sql.sources.partitionOverwriteMode=dynamic,spark.sql.session.timeZone=UTC",
    ]
    if subnet:
        command.append(f"--subnet={subnet}")
    job_args = [f"--processing-date={processing_date}", f"--output-path={output}", f"--stream-input={stream}",
                f"--hist-input={hist}"]
    for flag, name in (("--zones-input", "SPARK_ZONES_INPUT"), ("--driver-snapshot", "SPARK_DRIVER_SNAPSHOT"),
                       ("--timezone", "SPARK_TIMEZONE")):
        if get(name):
            job_args.append(f"{flag}={get(name)}")
    return command + ["--"] + job_args + (extra_args or [])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ridespark.submit", description=__doc__)
    parser.add_argument("--processing-date", required=True)
    args, extra = parser.parse_known_args(argv)
    try:
        command = build_command(dict(os.environ), args.processing_date, extra)
    except SubmitConfigError as exc:
        for problem in exc.problems:
            print(f"Configuration error: {problem}", file=sys.stderr)
        return 2
    print(" ".join(shlex.quote(part) for part in command))
    return 0


if __name__ == "__main__":
    sys.exit(main())
