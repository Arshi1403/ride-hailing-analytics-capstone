"""Command line entry point: `python -m producer --help`."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime

from .config import (
    DEFAULT_COUNT, DEFAULT_DURATION_SECONDS, DEFAULT_PROJECT, DEFAULT_TOPIC, ConfigError, ProducerConfig,
    validate_config,
)
from .logging_setup import configure_logging, log
from .publisher import DryRunPublisher, PubSubPublisher
from .runner import run
from .schema import parse_ts

EPILOG = """
safe progression (nothing is published unless --publish is given):
  1. python -m producer --count 10                       dry run, prints samples
  2. python -m producer --count 10 --duration-seconds 10 --publish        tiny real test
  3. python -m producer --publish --confirm-full-run     full 1000 events over 600 s (needs approval)
"""


def _env(name: str, default):
    value = os.environ.get(name)
    return default if value in (None, "") else value


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m producer", description="Publish mock ride events to Pub/Sub.",
                                epilog=EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--project-id", default=_env("GCP_PROJECT_ID", DEFAULT_PROJECT), help="env GCP_PROJECT_ID")
    p.add_argument("--topic", default=_env("PUBSUB_TOPIC", DEFAULT_TOPIC), help="topic short name, env PUBSUB_TOPIC")
    p.add_argument("--count", type=int, default=int(_env("PRODUCER_COUNT", DEFAULT_COUNT)), help="env PRODUCER_COUNT")
    p.add_argument("--duration-seconds", type=float,
                   default=float(_env("PRODUCER_DURATION_SECONDS", DEFAULT_DURATION_SECONDS)),
                   help="spread events evenly over this time, env PRODUCER_DURATION_SECONDS")
    p.add_argument("--seed", type=int, default=int(_env("PRODUCER_SEED", 42)), help="env PRODUCER_SEED")
    p.add_argument("--publish", action="store_true", help="really publish to Pub/Sub (default is a dry run)")
    p.add_argument("--confirm-full-run", action="store_true",
                   help="required to publish more than 50 events; use only after user approval")
    p.add_argument("--inject-invalid", type=int, default=0, metavar="N",
                   help="corrupt N events to exercise the dead-letter path (max 5 when publishing)")
    p.add_argument("--start-time", help="first pickup_ts as YYYY-MM-DDTHH:MM:SSZ (default: now)")
    p.add_argument("--output", help="dry run: write all events to this JSONL file")
    p.add_argument("--print-sample", type=int, default=3, help="dry run: print this many events")
    p.add_argument("--log-level", default="INFO")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    logger = logging.getLogger("producer.cli")
    try:
        start_time = parse_ts(args.start_time) if args.start_time else None
        cfg = ProducerConfig(
            project_id=args.project_id, topic=args.topic, count=args.count, duration_seconds=args.duration_seconds,
            seed=args.seed, publish=args.publish, confirm_full_run=args.confirm_full_run,
            inject_invalid=args.inject_invalid, start_time=start_time, output_path=args.output,
            print_sample=args.print_sample,
        )
        validate_config(cfg)
    except (ConfigError, ValueError) as exc:
        log(logger, logging.ERROR, "invalid configuration", problem=str(exc))
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    try:
        if cfg.publish:
            publisher = PubSubPublisher(cfg.project_id, cfg.topic)
        else:
            publisher = DryRunPublisher()
        summary = run(cfg, publisher)
    except Exception as exc:  # noqa: BLE001 - last-resort readable error
        log(logger, logging.ERROR, "run failed", error=f"{type(exc).__name__}: {exc}")
        print(f"Run failed: {type(exc).__name__}: {exc}\n"
              "Check: gcloud auth application-default login, topic exists, and you may publish to it.", file=sys.stderr)
        return 1

    if isinstance(publisher, DryRunPublisher):
        for data, _ in publisher.messages[: cfg.print_sample]:
            print(data.decode("utf-8"))
        if cfg.output_path:
            with open(cfg.output_path, "w", encoding="utf-8") as fh:
                fh.writelines(data.decode("utf-8") + "\n" for data, _ in publisher.messages)
    print(json.dumps(summary.__dict__, indent=2))
    return 1 if summary.failed else 0


if __name__ == "__main__":
    sys.exit(main())
