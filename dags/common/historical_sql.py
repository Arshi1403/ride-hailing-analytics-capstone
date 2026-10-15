"""Historical NYC Taxi 2018 CSV (GCS) -> raw_ds.rides_history.

Two steps, matching the capstone's GCSToBigQueryOperator requirement while staying rerunnable:
1. load_history_to_raw  (GCSToBigQueryOperator): CSV -> raw_ds.rides_history_stg, replacing the staging table.
2. merge_history_to_raw (BigQueryInsertJobOperator): one transaction that deletes this source file's earlier rows and
   inserts the 2018 rows. Same logic as ingestion/historical.py (the CSV header is checked there, not here).
The column list must equal ingestion/schemas.py HIST_CSV_COLUMNS; tests/dags checks it.
"""
from __future__ import annotations

HIST_COLUMNS = (
    ("VendorID", "INT64"), ("tpep_pickup_datetime", "TIMESTAMP"), ("tpep_dropoff_datetime", "TIMESTAMP"),
    ("passenger_count", "INT64"), ("trip_distance", "FLOAT64"), ("RatecodeID", "INT64"),
    ("store_and_fwd_flag", "STRING"), ("PULocationID", "INT64"), ("DOLocationID", "INT64"), ("payment_type", "INT64"),
    ("fare_amount", "NUMERIC"), ("extra", "NUMERIC"), ("mta_tax", "NUMERIC"), ("tip_amount", "NUMERIC"),
    ("tolls_amount", "NUMERIC"), ("improvement_surcharge", "NUMERIC"), ("total_amount", "NUMERIC"),
)
HIST_YEAR = 2018
STAGING_TABLE = "raw_ds.rides_history_stg"
PARAMS = ("source_file", "load_id")


def schema_fields() -> list[dict]:
    return [{"name": name, "type": col_type, "mode": "NULLABLE"} for name, col_type in HIST_COLUMNS]


def object_template() -> str:
    return "{{ rh_historical_object(ds, params) }}"


def merge_script() -> str:
    names = ", ".join(name for name, _ in HIST_COLUMNS)
    stg = STAGING_TABLE
    return f"""
ASSERT (SELECT COUNT(*) FROM {stg}) > 0
  AS 'the historical CSV has no data rows: check the object name and that it is the NYC TLC layout';
ASSERT (SELECT COUNTIF(EXTRACT(YEAR FROM tpep_pickup_datetime) = {HIST_YEAR}) FROM {stg}) > 0
  AS 'the historical CSV has no {HIST_YEAR} pickups: the capstone needs NYC Taxi {HIST_YEAR}, no other year is substituted';

BEGIN TRANSACTION;

DELETE FROM raw_ds.rides_history
WHERE source_file = @source_file
  AND DATE(tpep_pickup_datetime) BETWEEN
        (SELECT MIN(DATE(tpep_pickup_datetime)) FROM {stg} WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = {HIST_YEAR})
    AND (SELECT MAX(DATE(tpep_pickup_datetime)) FROM {stg} WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = {HIST_YEAR});

INSERT INTO raw_ds.rides_history ({names}, source_file, load_id, ingest_ts)
SELECT {names}, @source_file, @load_id, CURRENT_TIMESTAMP()
FROM {stg}
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = {HIST_YEAR};

COMMIT TRANSACTION;
"""


def merge_configuration(settings) -> dict:
    return {
        "query": {
            "query": merge_script(),
            "useLegacySql": False,
            "maximumBytesBilled": str(settings.max_bytes_billed),
            "parameterMode": "NAMED",
            "queryParameters": [
                {"name": "source_file", "parameterType": {"type": "STRING"},
                 "parameterValue": {"value": f"gs://{settings.landing_bucket}/{object_template()}"}},
                {"name": "load_id", "parameterType": {"type": "STRING"},
                 "parameterValue": {"value": "{{ rh_run_id(ds, ts_nodash) }}"}},
            ],
        },
        "labels": {"pipeline": "ride_hailing", "dag": "ride_hailing_daily", "task": "merge-history-to-raw"},
    }
