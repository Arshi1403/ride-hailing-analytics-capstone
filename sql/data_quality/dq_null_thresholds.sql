-- Purpose: null-rate checks on curated rides. Required columns must never be null; optional ones have small allowances.
-- Params: @run_id STRING, @window_start DATE, @window_end DATE
-- Cost: REQUIRES APPROVAL for the first run. Scans the listed columns of the window's partitions only. Dry-run first.
-- Output: ops_ds.dq_results columns. threshold = maximum allowed null rate. Rows are per column.
-- Notes: driver_id is checked on stream rides only (historical NYC rides have no driver by design).
--   Zone names can be null when the zone dimension has no match; that is also reported by dq_missing_dimensions.sql.

WITH stats AS (
  SELECT
    COUNT(*) AS n,
    COUNTIF(source = 'stream') AS n_stream,
    COUNTIF(ride_id IS NULL) AS ride_id,
    COUNTIF(source IS NULL) AS source,
    COUNTIF(pickup_ts IS NULL) AS pickup_ts,
    COUNTIF(dropoff_ts IS NULL) AS dropoff_ts,
    COUNTIF(fare IS NULL) AS fare,
    COUNTIF(pickup_date IS NULL) AS pickup_date,
    COUNTIF(trip_minutes IS NULL) AS trip_minutes,
    COUNTIF(hour_bucket IS NULL) AS hour_bucket,
    COUNTIF(weekday IS NULL) AS weekday,
    COUNTIF(pickup_zone IS NULL) AS pickup_zone,
    COUNTIF(dropoff_zone IS NULL) AS dropoff_zone,
    COUNTIF(pickup_zone_name IS NULL) AS pickup_zone_name,
    COUNTIF(source = 'stream' AND driver_id IS NULL) AS stream_driver_id,
    COUNTIF(source = 'stream' AND surge_multiplier IS NULL) AS stream_surge
  FROM curated_ds.fact_rides
  WHERE pickup_date BETWEEN @window_start AND @window_end
),
checks AS (
  SELECT
    CONCAT('DQ-NL-', c.column_name) AS check_id,
    CONCAT('null rate of ', c.column_name) AS check_name,
    c.severity,
    SAFE_DIVIDE(c.null_count, c.total) AS observed_value,
    c.max_rate AS threshold,
    COALESCE(SAFE_DIVIDE(c.null_count, c.total), 0) <= c.max_rate AS passed,
    CONCAT(CAST(c.null_count AS STRING), ' null of ', CAST(c.total AS STRING)) AS detail
  FROM stats, UNNEST([
    STRUCT('ride_id' AS column_name, ride_id AS null_count, n AS total, 0.0 AS max_rate, 'ERROR' AS severity),
    STRUCT('source', source, n, 0.0, 'ERROR'),
    STRUCT('pickup_ts', pickup_ts, n, 0.0, 'ERROR'),
    STRUCT('dropoff_ts', dropoff_ts, n, 0.0, 'ERROR'),
    STRUCT('fare', fare, n, 0.0, 'ERROR'),
    STRUCT('pickup_date', pickup_date, n, 0.0, 'ERROR'),
    STRUCT('trip_minutes', trip_minutes, n, 0.0, 'ERROR'),
    STRUCT('hour_bucket', hour_bucket, n, 0.0, 'ERROR'),
    STRUCT('weekday', weekday, n, 0.0, 'ERROR'),
    STRUCT('pickup_zone', pickup_zone, n, 0.01, 'ERROR'),
    STRUCT('dropoff_zone', dropoff_zone, n, 0.01, 'WARN'),
    STRUCT('pickup_zone_name', pickup_zone_name, n, 0.05, 'WARN'),
    STRUCT('stream_driver_id', stream_driver_id, n_stream, 0.0, 'ERROR'),
    STRUCT('stream_surge_multiplier', stream_surge, n_stream, 0.0, 'WARN')
  ]) AS c
)
SELECT
  @run_id AS run_id,
  @window_start AS window_start,
  @window_end AS window_end,
  check_id,
  check_name,
  severity,
  IF(passed, 'PASS', IF(severity = 'ERROR', 'FAIL', 'WARN')) AS status,
  observed_value,
  threshold,
  detail,
  CURRENT_TIMESTAMP() AS checked_ts
FROM checks
ORDER BY check_id;
