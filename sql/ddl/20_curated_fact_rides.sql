-- Purpose: curated fact table, one row per valid, deduplicated ride. Written only by sql/merge/merge_fact_rides.sql.
-- Source of truth: spark/ridespark/schemas.py (CURATED_RIDES_SCHEMA). Column order, names and types must match; tests/sql checks it.
-- Params: none
-- Cost: metadata only. Approval needed (creates a table).
-- Partition: pickup_date (business date of the pickup in the Spark --timezone). Spark partitions its Parquet by the same column.
--   processing_date is an audit column (date of the run), not a partition.
-- Cluster: pickup_zone, driver_id (zone and driver filters/aggregations in the marts and leaderboards).
-- Not partition-filter-enforced on purpose: marts aggregate across all dates. Use maximum_bytes_billed when querying (see sql/README.md).
-- Null notes: driver_id/rider_id are null for historical rows; distance_km only exists for historical rows; surge_* only for stream rows.

CREATE TABLE IF NOT EXISTS curated_ds.fact_rides (
  ride_id STRING NOT NULL,
  source STRING NOT NULL OPTIONS (description = 'stream or historical'),
  source_file STRING,
  driver_id STRING,
  rider_id STRING,
  pickup_zone STRING,
  dropoff_zone STRING,
  pickup_ts TIMESTAMP NOT NULL,
  dropoff_ts TIMESTAMP NOT NULL,
  fare NUMERIC NOT NULL,
  surge_multiplier FLOAT64,
  distance_km FLOAT64,
  ingest_ts TIMESTAMP,
  trip_minutes FLOAT64,
  revenue_per_km FLOAT64 OPTIONS (description = 'fare / distance_km; null when no valid distance'),
  surge_flag BOOL OPTIONS (description = 'surge_multiplier > 1.0; null when surge unknown'),
  weekday STRING,
  hour_bucket STRING OPTIONS (description = 'NIGHT, MORNING_PEAK, MIDDAY, EVENING_PEAK, LATE_EVENING'),
  pickup_zone_name STRING,
  pickup_borough STRING,
  dropoff_zone_name STRING,
  dropoff_borough STRING,
  zone_match STRING OPTIONS (description = 'MATCHED, PICKUP_NOT_FOUND, DROPOFF_NOT_FOUND, BOTH_NOT_FOUND, DIMENSION_UNAVAILABLE'),
  driver_vehicle_type STRING,
  driver_home_zone STRING,
  driver_status STRING,
  driver_version_from DATE,
  driver_match STRING OPTIONS (description = 'MATCHED, NO_DRIVER_ID, DRIVER_NOT_FOUND, DIMENSION_UNAVAILABLE'),
  processing_date DATE NOT NULL OPTIONS (description = 'Audit: date the run is attributed to; not the partition'),
  run_id STRING NOT NULL,
  pickup_date DATE NOT NULL OPTIONS (description = 'Partition column: business date of the pickup')
)
PARTITION BY pickup_date
CLUSTER BY pickup_zone, driver_id
OPTIONS (description = 'Curated rides: validated, deduplicated, enriched. Written by sql/merge/merge_fact_rides.sql only.');
