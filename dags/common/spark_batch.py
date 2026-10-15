"""Dataproc Serverless batch definition for spark/run_job.py (contract: spark/README.md)."""
from __future__ import annotations


def batch_args(settings) -> list[str]:
    """Spark job arguments. Jinja macros resolve per run; values must stay valid for ridespark.config.parse_args."""
    args = [
        "--processing-date={{ ds }}",
        "--pickup-start-date={{ rh_window_start(ds, params) }}",
        "--pickup-end-date={{ ds }}",
        f"--timezone={settings.business_tz}",
        "--run-id={{ rh_run_id(ds, ts_nodash) }}",
        f"--output-path={settings.output_path}",
        f"--stream-input={settings.stream_input}",
        f"--hist-input={settings.hist_input}",
    ]
    if settings.zones_uri:
        args.append(f"--zones-input={settings.zones_uri}")
    if settings.driver_snapshot_uri:
        args += [f"--driver-snapshot={settings.driver_snapshot_uri}", "--snapshot-date={{ ds }}"]
    if settings.allow_missing_dimensions:
        args.append("--allow-missing-dimensions")
    return args


def batch_id_template() -> str:
    """Unique per run and per retry (try_number), so a retry or a cleared task never reuses a finished batch."""
    return "rides-curation-{{ ts_nodash | lower }}-{{ ti.try_number }}"


def batch(settings) -> dict:
    execution = {"service_account": settings.spark_service_account}
    if settings.spark_subnet:
        execution["subnetwork_uri"] = settings.spark_subnet
    return {
        "pyspark_batch": {
            "main_python_file_uri": settings.spark_main_uri,
            "python_file_uris": [settings.spark_zip_uri],
            "args": batch_args(settings),
        },
        "runtime_config": {
            "version": settings.spark_runtime,
            "properties": {
                "spark.sql.sources.partitionOverwriteMode": "dynamic",
                "spark.sql.session.timeZone": "UTC",
            },
        },
        "environment_config": {"execution_config": execution},
        "labels": {"pipeline": "ride-hailing", "dag": "ride-hailing-daily"},
    }
