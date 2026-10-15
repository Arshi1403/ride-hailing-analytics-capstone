"""Thin adapters over the Google provider hooks. Libraries are imported lazily so DAG files import without them
and unit tests can replace these classes with fakes."""
from __future__ import annotations

import datetime as dt
import decimal
import logging
from typing import Any

log = logging.getLogger("airflow.task")


class GcsAdapter:
    def __init__(self, settings):
        self._settings = settings

    def prefix_has_objects(self, bucket: str, prefix: str) -> bool:
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        hook = GCSHook(gcp_conn_id=self._settings.gcp_conn_id)
        return bool(hook.list(bucket_name=bucket, prefix=prefix, max_results=1))


class BigQueryAdapter:
    def __init__(self, settings):
        self._settings = settings
        self._client = None

    def _bq(self):
        if self._client is None:
            from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

            hook = BigQueryHook(gcp_conn_id=self._settings.gcp_conn_id, location=self._settings.bq_location, use_legacy_sql=False)
            self._client = hook.get_client(project_id=self._settings.project_id, location=self._settings.bq_location)
        return self._client

    def load_parquet(self, uris: list[str], table: str, hive_prefix: str) -> int:
        """Replace `table` with the Parquet files in `uris`; hive partition columns are read from the folder names."""
        from google.cloud import bigquery

        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        hive = bigquery.HivePartitioningOptions()
        hive.mode = "AUTO"
        hive.source_uri_prefix = hive_prefix
        config.hive_partitioning = hive
        job = self._bq().load_table_from_uri(uris, f"{self._settings.project_id}.{table}", job_config=config)
        job.result()
        return int(job.output_rows or 0)

    def empty_table(self, table: str) -> None:
        """Remove all rows of a staging table (scratch data reloaded on every run)."""
        self._bq().query(
            f"DELETE FROM `{self._settings.project_id}.{table}` WHERE TRUE",
            job_config=self._query_config([], 1_000_000_000),
        ).result()

    def run_query(self, sql: str, params: list[tuple[str, str, Any]], max_bytes: int) -> list[dict]:
        job = self._bq().query(sql, job_config=self._query_config(params, max_bytes))
        return [dict(row) for row in job.result()]

    def append_rows(self, table: str, rows: list[dict]) -> None:
        if not rows:
            return
        from google.cloud import bigquery

        config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        self._bq().load_table_from_json(
            [jsonable(row) for row in rows], f"{self._settings.project_id}.{table}", job_config=config
        ).result()

    def _query_config(self, params, max_bytes):
        from google.cloud import bigquery

        return bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(name, kind, value) for name, kind, value in params],
            maximum_bytes_billed=max_bytes,
            use_legacy_sql=False,
        )


def jsonable(row: dict) -> dict:
    out = {}
    for key, value in row.items():
        if isinstance(value, (dt.datetime, dt.date)):
            out[key] = value.isoformat()
        elif isinstance(value, decimal.Decimal):
            out[key] = float(value)
        else:
            out[key] = value
    return out
