-- Purpose: quarantine of rides the Spark job rejected, with the reason. Used for reject-rate and reconciliation checks.
-- Source of truth: spark/ridespark/schemas.py (REJECTED_SCHEMA). Order, names, types must match; tests/sql checks it.
-- Params: none
-- Cost: metadata only. Approval needed (creates a table).
-- Partition: event_date (pickup date, else ingest date, else processing date).

CREATE TABLE IF NOT EXISTS curated_ds.rejected_rides (
  ride_id STRING,
  source STRING NOT NULL,
  source_file STRING,
  driver_id STRING,
  rider_id STRING,
  pickup_zone STRING,
  dropoff_zone STRING,
  pickup_ts TIMESTAMP,
  dropoff_ts TIMESTAMP,
  fare NUMERIC,
  surge_multiplier FLOAT64,
  distance_km FLOAT64,
  ingest_ts TIMESTAMP,
  reject_reason STRING NOT NULL OPTIONS (description = 'NULL_RIDE_ID, NULL_PICKUP_TS, NULL_DROPOFF_TS, NULL_FARE, NEGATIVE_FARE, NEGATIVE_TRIP_DURATION, TRIP_TOO_LONG'),
  processing_date DATE NOT NULL,
  run_id STRING NOT NULL,
  event_date DATE NOT NULL
)
PARTITION BY event_date
OPTIONS (description = 'Rejected rides with reason. Written only by sql/merge/merge_rejected_rides.sql.');
