"""BigQuery raw table schemas and DDL rendering. Printing DDL never touches the cloud."""
from __future__ import annotations

import argparse
import os
import sys

from .config import ConfigError, IngestionConfig, load_config

# (name, BigQuery type, description). All streaming columns are REQUIRED.
STREAM_COLUMNS = (
    ("ride_id", "STRING", "Unique ride id (dedupe key; delivery is at-least-once)"),
    ("driver_id", "STRING", "Driver id"),
    ("rider_id", "STRING", "Rider id"),
    ("pickup_zone", "STRING", "NYC TLC taxi zone id (LocationID) of pickup"),
    ("dropoff_zone", "STRING", "NYC TLC taxi zone id (LocationID) of dropoff"),
    ("pickup_ts", "TIMESTAMP", "Pickup time, UTC; producer sends YYYY-MM-DDTHH:MM:SSZ"),
    ("dropoff_ts", "TIMESTAMP", "Dropoff time, UTC"),
    ("fare", "NUMERIC", "Fare amount, INR, > 0 and <= 1000"),
    ("surge_multiplier", "FLOAT64", "Surge multiplier, 1.0 to 5.0"),
    ("ingest_ts", "TIMESTAMP", "Ingestion boundary time, UTC; added by the Dataflow UDF, not by the producer"),
)

# NYC TLC yellow taxi 2018 CSV header, in file order.
HIST_CSV_COLUMNS = (
    ("VendorID", "INT64"),
    ("tpep_pickup_datetime", "TIMESTAMP"),
    ("tpep_dropoff_datetime", "TIMESTAMP"),
    ("passenger_count", "INT64"),
    ("trip_distance", "FLOAT64"),
    ("RatecodeID", "INT64"),
    ("store_and_fwd_flag", "STRING"),
    ("PULocationID", "INT64"),
    ("DOLocationID", "INT64"),
    ("payment_type", "INT64"),
    ("fare_amount", "NUMERIC"),
    ("extra", "NUMERIC"),
    ("mta_tax", "NUMERIC"),
    ("tip_amount", "NUMERIC"),
    ("tolls_amount", "NUMERIC"),
    ("improvement_surcharge", "NUMERIC"),
    ("total_amount", "NUMERIC"),
)
HIST_META_COLUMNS = (
    ("source_file", "STRING", "gs:// URI of the CSV this row came from (rerun key)"),
    ("load_id", "STRING", "Id of the load run that wrote the row"),
    ("ingest_ts", "TIMESTAMP", "Time the row was loaded into raw, UTC"),
)


def _ddl(table_fq: str, columns: list[tuple[str, str, bool, str]], partition: str, cluster: str, description: str) -> str:
    lines = []
    for name, bq_type, required, desc in columns:
        not_null = " NOT NULL" if required else ""
        suffix = f' OPTIONS(description="{desc}")' if desc else ""
        lines.append(f"  {name} {bq_type}{not_null}{suffix}")
    body = ",\n".join(lines)
    return (
        f"CREATE TABLE IF NOT EXISTS `{table_fq}` (\n{body}\n)\n"
        f"PARTITION BY {partition}\nCLUSTER BY {cluster}\n"
        f'OPTIONS(description="{description}");'
    )


def stream_ddl(cfg: IngestionConfig) -> str:
    cols = [(n, t, True, d) for n, t, d in STREAM_COLUMNS]
    return _ddl(cfg.fq_table(cfg.stream_table), cols, "DATE(ingest_ts)", "pickup_zone",
                "Raw ride events from Pub/Sub via Dataflow. Append-only; dedupe on ride_id downstream.")


def hist_ddl(cfg: IngestionConfig) -> str:
    cols = [(n, t, False, "") for n, t in HIST_CSV_COLUMNS]
    cols += [(n, t, True, d) for n, t, d in HIST_META_COLUMNS]
    return _ddl(cfg.fq_table(cfg.hist_table), cols, "DATE(tpep_pickup_datetime)", "PULocationID",
                f"Raw NYC Taxi {cfg.hist_year} yellow trips loaded from GCS CSV. Only {cfg.hist_year} pickups are loaded.")


def dataset_ddl(cfg: IngestionConfig) -> str:
    return f'CREATE SCHEMA IF NOT EXISTS `{cfg.project_id}.{cfg.raw_dataset}`\nOPTIONS(location="{cfg.region}");'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.schemas",
        description="Print CREATE statements for the raw tables. Nothing is executed; review, get approval, then run with `bq query`.",
    )
    parser.add_argument("what", choices=["dataset", "stream", "hist", "all"])
    args = parser.parse_args(argv)
    try:
        cfg = load_config(os.environ)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    parts = {"dataset": [dataset_ddl], "stream": [stream_ddl], "hist": [hist_ddl],
             "all": [dataset_ddl, stream_ddl, hist_ddl]}[args.what]
    print("\n\n".join(p(cfg) for p in parts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
