-- Purpose: raw historical NYC Taxi 2018 trips loaded from GCS CSV by ingestion/historical.py.
-- Source of truth: ingestion/schemas.py (hist_ddl). Columns must stay identical; tests/sql checks it.
-- Params: none
-- Cost: metadata only. Approval needed (creates a table).
-- Table: raw_ds.rides_history. source_file is the rerun key; only 2018 pickups are loaded.

CREATE TABLE IF NOT EXISTS raw_ds.rides_history (
  VendorID INT64,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INT64,
  trip_distance FLOAT64,
  RatecodeID INT64,
  store_and_fwd_flag STRING,
  PULocationID INT64,
  DOLocationID INT64,
  payment_type INT64,
  fare_amount NUMERIC,
  extra NUMERIC,
  mta_tax NUMERIC,
  tip_amount NUMERIC,
  tolls_amount NUMERIC,
  improvement_surcharge NUMERIC,
  total_amount NUMERIC,
  source_file STRING NOT NULL OPTIONS (description = 'gs:// URI of the CSV this row came from (rerun key)'),
  load_id STRING NOT NULL OPTIONS (description = 'Id of the load run that wrote the row'),
  ingest_ts TIMESTAMP NOT NULL OPTIONS (description = 'Time the row was loaded into raw, UTC')
)
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY PULocationID
OPTIONS (description = 'Raw NYC Taxi 2018 yellow trips loaded from GCS CSV. Only 2018 pickups are loaded.');
