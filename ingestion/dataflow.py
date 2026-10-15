"""Print the gcloud command that launches the Google-provided Pub/Sub Subscription to BigQuery template.

This module never starts a job. Review the printed command, get approval, then run it yourself.
"""
from __future__ import annotations

import argparse
import logging
import os
import shlex
import sys

from .config import ConfigError, IngestionConfig, load_config, require_for_dataflow
from .logging_setup import configure_logging, log

logger = logging.getLogger("ingestion.dataflow")


def build_parameters(cfg: IngestionConfig) -> dict[str, str]:
    """Template parameters. The input is a SUBSCRIPTION on purpose (see ingestion/README.md)."""
    return {
        "inputSubscription": f"projects/{cfg.project_id}/subscriptions/{cfg.subscription}",
        "outputTableSpec": cfg.stream_table_spec,
        "outputDeadletterTable": cfg.dlq_table_spec,
        "javascriptTextTransformGcsPath": cfg.udf_gcs_path,
        "javascriptTextTransformFunctionName": cfg.udf_function_name,
    }


def build_gcloud_command(cfg: IngestionConfig) -> list[str]:
    require_for_dataflow(cfg)
    params = ",".join(f"{k}={v}" for k, v in build_parameters(cfg).items())
    return [
        "gcloud", "dataflow", "jobs", "run", cfg.job_name,
        f"--project={cfg.project_id}",
        f"--region={cfg.region}",
        f"--gcs-location={cfg.template_path}",
        f"--staging-location={cfg.temp_location}",
        f"--service-account-email={cfg.service_account_email}",
        f"--max-workers={cfg.max_workers}",
        f"--parameters={params}",
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ingestion.dataflow", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    try:
        cfg = load_config(os.environ)
        command = build_gcloud_command(cfg)
    except ConfigError as exc:
        for problem in exc.problems:
            print(f"Configuration error: {problem}", file=sys.stderr)
        return 2
    log(logger, logging.INFO, "launch command generated (not executed)", job_name=cfg.job_name,
        subscription=cfg.subscription, output_table=cfg.stream_table_spec)
    print(" ".join(shlex.quote(part) for part in command))
    return 0


if __name__ == "__main__":
    sys.exit(main())
