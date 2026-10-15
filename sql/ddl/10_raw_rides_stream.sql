-- Purpose: raw streaming rides written by the Dataflow Pub/Sub-to-BigQuery template.
-- Source of truth: ingestion/schemas.py (stream_ddl). This file must stay identical in columns; tests/sql checks it.
-- Params: none
-- Cost: metadata only. Approval needed (creates a table). Table is append-only; dedupe happens in Spark.
-- Table: raw_ds.rides_stream  (ride_id is the dedupe key; delivery is at-least-once)

CREATE TABLE IF NOT EXISTS raw_ds.rides_stream (
  ride_id STRING NOT NULL OPTIONS (description = 'Unique ride id (dedupe key; delivery is at-least-once)'),
  driver_id STRING NOT NULL OPTIONS (description = 'Driver id'),
  rider_id STRING NOT NULL OPTIONS (description = 'Rider id'),
  pickup_zone STRING NOT NULL OPTIONS (description = 'NYC TLC taxi zone id (LocationID) of pickup'),
  dropoff_zone STRING NOT NULL OPTIONS (description = 'NYC TLC taxi zone id (LocationID) of dropoff'),
  pickup_ts TIMESTAMP NOT NULL OPTIONS (description = 'Pickup time, UTC'),
  dropoff_ts TIMESTAMP NOT NULL OPTIONS (description = 'Dropoff time, UTC'),
  fare NUMERIC NOT NULL OPTIONS (description = 'Fare amount, INR, > 0 and <= 1000'),
  surge_multiplier FLOAT64 NOT NULL OPTIONS (description = 'Surge multiplier, 1.0 to 5.0'),
  ingest_ts TIMESTAMP NOT NULL OPTIONS (description = 'Ingestion boundary time, UTC; added by the Dataflow UDF')
)
PARTITION BY DATE(ingest_ts)
CLUSTER BY pickup_zone
OPTIONS (description = 'Raw ride events from Pub/Sub via Dataflow. Append-only; dedupe on ride_id downstream.');
