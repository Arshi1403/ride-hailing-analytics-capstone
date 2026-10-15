-- Purpose: staging tables loaded from the Spark Parquet output (one run at a time), then merged into curated_ds.
-- Source of truth: spark/ridespark/schemas.py. Staging columns are the Parquet columns; all nullable because Parquet is.
-- Params: none
-- Cost: metadata only. Approval needed (creates tables).
-- Load (Composer, WRITE_TRUNCATE, hive partitioning is read by the load job from the Parquet folder layout):
--   rides:     gs://<curated>/rides/pickup_date=.../   -> stg_ds.fact_rides_stg
--   rejected:  gs://<curated>/rejected/event_date=.../ -> stg_ds.rejected_rides_stg
--   scd2:      gs://<curated>/dim_driver_scd2/snapshot_date=<snapshot date>/ -> stg_ds.dim_driver_scd2_stg  (load ONLY the latest snapshot_date partition)
-- Staging must hold exactly one run's output: the merge scripts assert this before changing curated tables.

CREATE TABLE IF NOT EXISTS stg_ds.fact_rides_stg (
  ride_id STRING, source STRING, source_file STRING, driver_id STRING, rider_id STRING,
  pickup_zone STRING, dropoff_zone STRING, pickup_ts TIMESTAMP, dropoff_ts TIMESTAMP, fare NUMERIC,
  surge_multiplier FLOAT64, distance_km FLOAT64, ingest_ts TIMESTAMP, trip_minutes FLOAT64,
  revenue_per_km FLOAT64, surge_flag BOOL, weekday STRING, hour_bucket STRING,
  pickup_zone_name STRING, pickup_borough STRING, dropoff_zone_name STRING, dropoff_borough STRING,
  zone_match STRING, driver_vehicle_type STRING, driver_home_zone STRING, driver_status STRING,
  driver_version_from DATE, driver_match STRING, processing_date DATE, run_id STRING, pickup_date DATE
)
OPTIONS (description = 'Spark output for one run (rides). Truncated on every load.');

CREATE TABLE IF NOT EXISTS stg_ds.rejected_rides_stg (
  ride_id STRING, source STRING, source_file STRING, driver_id STRING, rider_id STRING,
  pickup_zone STRING, dropoff_zone STRING, pickup_ts TIMESTAMP, dropoff_ts TIMESTAMP, fare NUMERIC,
  surge_multiplier FLOAT64, distance_km FLOAT64, ingest_ts TIMESTAMP,
  reject_reason STRING, processing_date DATE, run_id STRING, event_date DATE
)
OPTIONS (description = 'Spark output for one run (rejected rides). Truncated on every load.');

CREATE TABLE IF NOT EXISTS stg_ds.dim_driver_scd2_stg (
  driver_id STRING, vehicle_type STRING, home_zone STRING, status STRING, record_hash STRING,
  effective_from DATE, effective_to DATE, is_current BOOL, snapshot_date DATE
)
OPTIONS (description = 'Full driver history as of one snapshot date (Spark output). Truncated on every load.');
