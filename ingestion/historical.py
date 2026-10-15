"""Historical NYC Taxi 2018 CSV (GCS) -> BigQuery raw, with safe reruns.

Flow per file: header check -> load into a temporary staging table -> year check ->
one transaction that deletes this file's earlier rows and inserts the 2018 rows -> drop staging.
Default is a dry run that prints the plan; add --execute to touch BigQuery.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from .config import GCS_URI_RE, ConfigError, IngestionConfig, load_config
from .logging_setup import configure_logging, log
from .schemas import HIST_CSV_COLUMNS, hist_ddl

logger = logging.getLogger("ingestion.historical")
CSV_COLUMN_NAMES = [name for name, _ in HIST_CSV_COLUMNS]


class LoadError(Exception):
    """A problem the user can fix; the message says how."""


class Gateway(Protocol):
    def table_exists(self, table_fq: str) -> bool: ...
    def read_header(self, gcs_uri: str) -> str: ...
    def load_csv(self, gcs_uri: str, table_fq: str, max_bad_records: int) -> int: ...
    def query(self, sql: str, params: dict[str, Any]) -> list[dict]: ...
    def delete_table(self, table_fq: str) -> None: ...


@dataclass(frozen=True)
class LoadPlan:
    gcs_uri: str
    target_table: str
    staging_table: str
    load_id: str
    year: int
    sample_percent: int
    max_bad_records: int
    stats_sql: str
    replace_sql: str


@dataclass
class LoadResult:
    load_id: str
    source_file: str
    staged_rows: int
    rows_in_year: int
    rows_outside_year: int
    rows_loaded: int
    sample_percent: int


def validate_gcs_uri(uri: str, pattern: str) -> None:
    if not GCS_URI_RE.match(uri) or uri.endswith("/"):
        raise LoadError(f"{uri!r} must be a gs://bucket/path/file.csv object URI (one file per run, no wildcards)")
    if "*" in uri:
        raise LoadError("wildcards are not supported: the source file is the rerun key; load one file at a time")
    if not re.match(pattern, uri.rsplit("/", 1)[-1]):
        raise LoadError(
            f"file name does not match HIST_FILE_PATTERN {pattern!r}. "
            "This project needs NYC Taxi 2018 data; no other year is substituted."
        )


def check_header(header_line: str) -> None:
    got = [c.strip().strip('"').lower() for c in header_line.strip().split(",")]
    want = [c.lower() for c in CSV_COLUMN_NAMES]
    if got != want:
        raise LoadError(
            "CSV header does not match the NYC TLC 2018 yellow-taxi layout.\n"
            f"  expected: {','.join(CSV_COLUMN_NAMES)}\n  found:    {header_line.strip()}\n"
            "If the file was exported from the BigQuery public dataset, its column names differ; export the TLC CSV format."
        )


def _select_list() -> str:
    return ", ".join(CSV_COLUMN_NAMES)


def build_plan(cfg: IngestionConfig, gcs_uri: str, sample_percent: int = 100, max_bad_records: int = 0,
               now: datetime | None = None) -> LoadPlan:
    validate_gcs_uri(gcs_uri, cfg.hist_file_pattern)
    if not 1 <= sample_percent <= 100:
        raise LoadError("sample percent must be between 1 and 100")
    if max_bad_records < 0:
        raise LoadError("max bad records must be >= 0")
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    load_id = f"{stamp}-{uuid.uuid4().hex[:8]}"
    target = cfg.fq_table(cfg.hist_table)
    staging = cfg.fq_table(f"{cfg.hist_table}_stg_{load_id.replace('-', '_').lower()}")
    stats_sql = (
        f"SELECT COUNT(*) AS staged_rows, "
        f"COUNTIF(EXTRACT(YEAR FROM tpep_pickup_datetime) = @year) AS rows_in_year FROM `{staging}`"
    )
    replace_sql = (
        "BEGIN TRANSACTION;\n"
        f"DELETE FROM `{target}` WHERE source_file = @source_file;\n"
        f"INSERT INTO `{target}` ({_select_list()}, source_file, load_id, ingest_ts)\n"
        f"SELECT {_select_list()}, @source_file, @load_id, CURRENT_TIMESTAMP()\n"
        f"FROM `{staging}` AS s\n"
        "WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = @year\n"
        "  AND MOD(ABS(FARM_FINGERPRINT(TO_JSON_STRING(s))), 100) < @sample_percent;\n"
        "COMMIT TRANSACTION;"
    )
    return LoadPlan(gcs_uri, target, staging, load_id, cfg.hist_year, sample_percent, max_bad_records,
                    stats_sql, replace_sql)


def execute(plan: LoadPlan, gateway: Gateway) -> LoadResult:
    if not gateway.table_exists(plan.target_table):
        raise LoadError(
            f"target table {plan.target_table} does not exist. Print its DDL with "
            "`python -m ingestion.schemas hist`, get approval, create it, then rerun."
        )
    check_header(gateway.read_header(plan.gcs_uri))
    log(logger, logging.INFO, "header ok, loading to staging", load_id=plan.load_id, source=plan.gcs_uri)

    staged_loaded = False
    try:
        gateway.load_csv(plan.gcs_uri, plan.staging_table, plan.max_bad_records)
        staged_loaded = True
        stats = gateway.query(plan.stats_sql, {"year": plan.year})[0]
        staged, in_year = int(stats["staged_rows"]), int(stats["rows_in_year"])
        log(logger, logging.INFO, "staging loaded", load_id=plan.load_id, staged_rows=staged,
            rows_in_year=in_year, rows_outside_year=staged - in_year)
        if in_year == 0:
            raise LoadError(
                f"the file has no rows with a {plan.year} pickup (staged {staged} rows). Nothing was written. "
                "Check that you picked the NYC Taxi 2018 file."
            )
        gateway.query(plan.replace_sql, {"source_file": plan.gcs_uri, "load_id": plan.load_id,
                                         "year": plan.year, "sample_percent": plan.sample_percent})
        loaded = int(gateway.query(
            f"SELECT COUNT(*) AS n FROM `{plan.target_table}` WHERE source_file = @source_file",
            {"source_file": plan.gcs_uri})[0]["n"])
    finally:
        if staged_loaded:
            gateway.delete_table(plan.staging_table)
    result = LoadResult(plan.load_id, plan.gcs_uri, staged, in_year, staged - in_year, loaded, plan.sample_percent)
    log(logger, logging.INFO, "load complete", **result.__dict__)
    return result


class BigQueryGateway:
    """Real BigQuery/GCS access with Application Default Credentials. Not unit tested; see README validations."""

    def __init__(self, project_id: str, region: str) -> None:
        try:
            from google.cloud import bigquery, storage
        except ImportError as exc:
            raise RuntimeError("Missing libraries. Run: pip install -r ingestion/requirements.txt") from exc
        self._bq = bigquery
        self._client = bigquery.Client(project=project_id, location=region)
        self._storage = storage.Client(project=project_id)

    def table_exists(self, table_fq: str) -> bool:
        from google.api_core.exceptions import NotFound
        try:
            self._client.get_table(table_fq)
            return True
        except NotFound:
            return False

    def read_header(self, gcs_uri: str) -> str:
        bucket, _, name = gcs_uri[len("gs://"):].partition("/")
        blob = self._storage.bucket(bucket).blob(name)
        if name.endswith(".gz"):
            import gzip
            return gzip.decompress(blob.download_as_bytes()).split(b"\n", 1)[0].decode("utf-8")
        return blob.download_as_bytes(start=0, end=4095).split(b"\n", 1)[0].decode("utf-8")

    def load_csv(self, gcs_uri: str, table_fq: str, max_bad_records: int) -> int:
        bq = self._bq
        config = bq.LoadJobConfig(
            source_format=bq.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=[bq.SchemaField(n, t) for n, t in HIST_CSV_COLUMNS],
            write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
            max_bad_records=max_bad_records,
        )
        job = self._client.load_table_from_uri(gcs_uri, table_fq, job_config=config)
        job.result()
        table = self._client.get_table(table_fq)
        table.expires = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(hours=24)
        self._client.update_table(table, ["expires"])
        return int(job.output_rows or 0)

    def query(self, sql: str, params: dict[str, Any]) -> list[dict]:
        bq = self._bq
        query_params = [
            bq.ScalarQueryParameter(k, "INT64" if isinstance(v, int) else "STRING", v) for k, v in params.items()
        ]
        job = self._client.query(sql, job_config=bq.QueryJobConfig(query_parameters=query_params))
        return [dict(row) for row in job.result()]

    def delete_table(self, table_fq: str) -> None:
        self._client.delete_table(table_fq, not_found_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ingestion.historical", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gcs-uri", required=True, help="gs://bucket/path/yellow_tripdata_2018-01.csv (one file)")
    parser.add_argument("--sample-percent", type=int, default=100,
                        help="deterministic subset: keep ~N%% of rows (default 100)")
    parser.add_argument("--max-bad-records", type=int, default=0, help="malformed CSV rows tolerated (default 0)")
    parser.add_argument("--execute", action="store_true", help="really load into BigQuery (default: print the plan)")
    parser.add_argument("--print-ddl", action="store_true", help="also print the target table DDL")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    configure_logging(args.log_level)

    try:
        cfg = load_config(os.environ)
        plan = build_plan(cfg, args.gcs_uri, args.sample_percent, args.max_bad_records)
    except ConfigError as exc:
        for problem in exc.problems:
            print(f"Configuration error: {problem}", file=sys.stderr)
        return 2
    except LoadError as exc:
        print(f"Input error: {exc}", file=sys.stderr)
        return 2

    if not args.execute:
        print("DRY RUN: nothing was read or written. Add --execute to load.\n")
        print(json.dumps({k: v for k, v in plan.__dict__.items() if k not in ("stats_sql", "replace_sql")}, indent=2))
        print("\n-- stats query\n" + plan.stats_sql + "\n\n-- replace script\n" + plan.replace_sql)
        if args.print_ddl:
            print("\n-- target DDL\n" + hist_ddl(cfg))
        return 0

    try:
        result = execute(plan, BigQueryGateway(cfg.project_id, cfg.region))
    except LoadError as exc:
        log(logger, logging.ERROR, "load rejected", error=str(exc))
        print(f"Load failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        log(logger, logging.ERROR, "load crashed", error=f"{type(exc).__name__}: {exc}")
        print(f"Load failed: {type(exc).__name__}: {exc}\n"
              "Check: gcloud auth application-default login, bucket and dataset exist, roles in ingestion/README.md.",
              file=sys.stderr)
        return 1
    print(json.dumps(result.__dict__, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
