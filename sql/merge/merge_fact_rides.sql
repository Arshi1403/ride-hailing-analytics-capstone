-- Purpose: idempotent upsert of one Spark run's rides into curated_ds.fact_rides.
-- Params: @run_id STRING, @window_start DATE, @window_end DATE  (the pickup-date window the Spark run recomputed)
-- Cost: scans only the partitions in the window (the window is in the ON clause and the delete clause). Needs approval to run.
-- Inputs: stg_ds.fact_rides_stg must hold exactly this run's output (see sql/ddl/30_staging_tables.sql).
-- Rerun safety:
--   * Same run_id and same content: nothing changes (no matched row passes the update condition).
--   * Same window, new run: rows are updated to the new values and audit columns; rides that no longer exist in the
--     recomputed window are deleted, because Spark recomputes whole pickup dates from raw.
--   * Dates outside the window are never touched.
-- Failure guard: the ASSERTs stop the script before any change if staging is empty, mixed, or outside the window.

ASSERT (SELECT COUNT(*) FROM stg_ds.fact_rides_stg) > 0
  AS 'stg_ds.fact_rides_stg is empty: load the Spark output for this run first';
ASSERT (SELECT COUNTIF(run_id != @run_id OR run_id IS NULL) FROM stg_ds.fact_rides_stg) = 0
  AS 'stg_ds.fact_rides_stg contains rows from a different run_id';
ASSERT (SELECT COUNTIF(pickup_date IS NULL OR pickup_date NOT BETWEEN @window_start AND @window_end) FROM stg_ds.fact_rides_stg) = 0
  AS 'stg_ds.fact_rides_stg has pickup_date outside the window';
ASSERT NOT EXISTS (SELECT 1 FROM stg_ds.fact_rides_stg GROUP BY ride_id, pickup_date HAVING COUNT(*) > 1)
  AS 'stg_ds.fact_rides_stg has duplicate ride_id values';

MERGE curated_ds.fact_rides AS T
USING stg_ds.fact_rides_stg AS S
ON T.ride_id = S.ride_id
   AND T.pickup_date = S.pickup_date
   AND T.pickup_date BETWEEN @window_start AND @window_end
WHEN MATCHED AND (
     T.run_id != S.run_id
  OR T.fare IS DISTINCT FROM S.fare
  OR T.pickup_ts IS DISTINCT FROM S.pickup_ts
  OR T.dropoff_ts IS DISTINCT FROM S.dropoff_ts
  OR T.driver_id IS DISTINCT FROM S.driver_id
  OR T.pickup_zone IS DISTINCT FROM S.pickup_zone
  OR T.dropoff_zone IS DISTINCT FROM S.dropoff_zone
  OR T.zone_match IS DISTINCT FROM S.zone_match
  OR T.driver_match IS DISTINCT FROM S.driver_match
  OR T.driver_vehicle_type IS DISTINCT FROM S.driver_vehicle_type
) THEN UPDATE SET
  source = S.source,
  source_file = S.source_file,
  driver_id = S.driver_id,
  rider_id = S.rider_id,
  pickup_zone = S.pickup_zone,
  dropoff_zone = S.dropoff_zone,
  pickup_ts = S.pickup_ts,
  dropoff_ts = S.dropoff_ts,
  fare = S.fare,
  surge_multiplier = S.surge_multiplier,
  distance_km = S.distance_km,
  ingest_ts = S.ingest_ts,
  trip_minutes = S.trip_minutes,
  revenue_per_km = S.revenue_per_km,
  surge_flag = S.surge_flag,
  weekday = S.weekday,
  hour_bucket = S.hour_bucket,
  pickup_zone_name = S.pickup_zone_name,
  pickup_borough = S.pickup_borough,
  dropoff_zone_name = S.dropoff_zone_name,
  dropoff_borough = S.dropoff_borough,
  zone_match = S.zone_match,
  driver_vehicle_type = S.driver_vehicle_type,
  driver_home_zone = S.driver_home_zone,
  driver_status = S.driver_status,
  driver_version_from = S.driver_version_from,
  driver_match = S.driver_match,
  processing_date = S.processing_date,
  run_id = S.run_id
WHEN NOT MATCHED BY TARGET THEN INSERT (
  ride_id, source, source_file, driver_id, rider_id, pickup_zone, dropoff_zone, pickup_ts, dropoff_ts, fare,
  surge_multiplier, distance_km, ingest_ts, trip_minutes, revenue_per_km, surge_flag, weekday, hour_bucket,
  pickup_zone_name, pickup_borough, dropoff_zone_name, dropoff_borough, zone_match, driver_vehicle_type,
  driver_home_zone, driver_status, driver_version_from, driver_match, processing_date, run_id, pickup_date
) VALUES (
  S.ride_id, S.source, S.source_file, S.driver_id, S.rider_id, S.pickup_zone, S.dropoff_zone, S.pickup_ts, S.dropoff_ts, S.fare,
  S.surge_multiplier, S.distance_km, S.ingest_ts, S.trip_minutes, S.revenue_per_km, S.surge_flag, S.weekday, S.hour_bucket,
  S.pickup_zone_name, S.pickup_borough, S.dropoff_zone_name, S.dropoff_borough, S.zone_match, S.driver_vehicle_type,
  S.driver_home_zone, S.driver_status, S.driver_version_from, S.driver_match, S.processing_date, S.run_id, S.pickup_date
)
WHEN NOT MATCHED BY SOURCE AND T.pickup_date BETWEEN @window_start AND @window_end THEN DELETE;
