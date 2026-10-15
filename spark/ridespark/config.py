"""Command-line arguments and validation for the processing job."""
from __future__ import annotations

import argparse
import re
import uuid
from dataclasses import dataclass
from datetime import date, timedelta

BQ_SPEC_RE = re.compile(r"^bq://[a-z][a-z0-9-]{4,28}[a-z0-9]\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")
TZ_RE = re.compile(r"^(UTC|[A-Za-z]+(/[A-Za-z_+-]+){1,2})$")
RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
MAX_WINDOW_DAYS = 366


class JobConfigError(ValueError):
    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("; ".join(problems))


@dataclass(frozen=True)
class JobConfig:
    processing_date: date
    pickup_start: date
    pickup_end: date
    timezone: str
    run_id: str
    output_path: str
    stream_input: str | None
    hist_input: str | None
    zones_input: str | None
    driver_snapshot: str | None
    snapshot_date: date
    allow_missing_dimensions: bool


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="run_job.py", description="Curate raw ride data into partitioned Parquet.")
    p.add_argument("--processing-date", required=True, help="YYYY-MM-DD, the date this run is attributed to")
    p.add_argument("--lookback-days", type=int, default=3,
                   help="recompute pickup dates from processing-date minus N days (late data); default 3")
    p.add_argument("--pickup-start-date", help="override window start, YYYY-MM-DD")
    p.add_argument("--pickup-end-date", help="override window end (inclusive), YYYY-MM-DD")
    p.add_argument("--timezone", default="UTC", help="zone for pickup_date, weekday, hour_bucket; default UTC")
    p.add_argument("--stream-input", help="raw streaming rides: bq://project.dataset.table or Parquet path")
    p.add_argument("--hist-input", help="raw historical trips: bq://project.dataset.table or Parquet path")
    p.add_argument("--zones-input", help="zone dimension: CSV (TLC taxi_zone_lookup.csv), Parquet or bq://")
    p.add_argument("--driver-snapshot", help="daily driver snapshot: CSV, Parquet or bq://")
    p.add_argument("--snapshot-date", help="date of the driver snapshot; default processing-date")
    p.add_argument("--output-path", required=True, help="base path, gs://bucket/prefix or a local folder")
    p.add_argument("--run-id", help="default: <processing-date>-<random>")
    p.add_argument("--allow-missing-dimensions", action="store_true",
                   help="continue with null dimension attributes instead of failing")
    return p


def _parse_date(text: str, name: str, problems: list[str]) -> date | None:
    try:
        return date.fromisoformat(text)
    except (TypeError, ValueError):
        problems.append(f"{name} must be YYYY-MM-DD, got {text!r}")
        return None


def _check_source(spec: str | None, name: str, problems: list[str]) -> None:
    if spec is None:
        return
    if spec.startswith("bq://"):
        if not BQ_SPEC_RE.match(spec):
            problems.append(f"{name} must look like bq://project.dataset.table, got {spec!r}")
    elif not spec.strip():
        problems.append(f"{name} is empty")


def parse_args(argv: list[str] | None = None) -> JobConfig:
    args = build_parser().parse_args(argv)
    problems: list[str] = []

    processing = _parse_date(args.processing_date, "--processing-date", problems)
    snapshot = _parse_date(args.snapshot_date, "--snapshot-date", problems) if args.snapshot_date else processing
    start = _parse_date(args.pickup_start_date, "--pickup-start-date", problems) if args.pickup_start_date else None
    end = _parse_date(args.pickup_end_date, "--pickup-end-date", problems) if args.pickup_end_date else None
    if not 0 <= args.lookback_days <= 30:
        problems.append("--lookback-days must be between 0 and 30")

    if processing is not None and args.lookback_days >= 0:
        start = start or processing - timedelta(days=args.lookback_days)
        end = end or processing
    if start and end:
        if start > end:
            problems.append("pickup window start is after its end")
        elif (end - start).days + 1 > MAX_WINDOW_DAYS:
            problems.append(f"pickup window longer than {MAX_WINDOW_DAYS} days; run it in parts")
    if processing and snapshot and snapshot > processing:
        problems.append("--snapshot-date must not be after --processing-date")

    if not TZ_RE.match(args.timezone):
        problems.append(f"--timezone must be UTC or an IANA name like Asia/Kolkata, got {args.timezone!r}")
    run_id = args.run_id or f"{args.processing_date}-{uuid.uuid4().hex[:8]}"
    if not RUN_ID_RE.match(run_id):
        problems.append("--run-id may contain only letters, digits, '.', '_' and '-' (max 64)")
    if not (args.stream_input or args.hist_input):
        problems.append("give at least one of --stream-input or --hist-input")
    if not args.output_path.strip():
        problems.append("--output-path is empty")
    if args.driver_snapshot and not snapshot:
        problems.append("--snapshot-date is invalid")
    for spec, name in ((args.stream_input, "--stream-input"), (args.hist_input, "--hist-input"),
                       (args.zones_input, "--zones-input"), (args.driver_snapshot, "--driver-snapshot")):
        _check_source(spec, name, problems)
    if problems:
        raise JobConfigError(problems)

    return JobConfig(
        processing_date=processing, pickup_start=start, pickup_end=end, timezone=args.timezone, run_id=run_id,
        output_path=args.output_path, stream_input=args.stream_input, hist_input=args.hist_input,
        zones_input=args.zones_input, driver_snapshot=args.driver_snapshot, snapshot_date=snapshot,
        allow_missing_dimensions=args.allow_missing_dimensions,
    )
