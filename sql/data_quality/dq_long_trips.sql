-- Purpose: trip-duration checks: long trips (over 4 hours), negative durations, timestamp consistency.
-- Params: @run_id STRING, @window_start DATE, @window_end DATE
-- Cost: REQUIRES APPROVAL for the first run. Reads pickup/dropoff/trip_minutes of the window's partitions. Dry-run first.
-- Output: ops_ds.dq_results columns.
-- Rules: Spark rejects trips longer than 4 hours (240 minutes) and trips that end before they start, so none may reach curated data.
DECLARE max_trip_minutes FLOAT64 DEFAULT 240.0;
DECLARE max_zero_minute_share FLOAT64 DEFAULT 0.02;

WITH trips AS (
  SELECT
    COUNT(*) AS n,
    COUNTIF(trip_minutes > max_trip_minutes) AS too_long,
    COUNTIF(trip_minutes < 0) AS negative_minutes,
    COUNTIF(dropoff_ts < pickup_ts) AS dropoff_before_pickup,
    COUNTIF(trip_minutes = 0) AS zero_minutes,
    COUNTIF(ABS(trip_minutes - TIMESTAMP_DIFF(dropoff_ts, pickup_ts, SECOND) / 60.0) > 0.01) AS minutes_mismatch
  FROM curated_ds.fact_rides
  WHERE pickup_date BETWEEN @window_start AND @window_end
),
rejected_long AS (
  SELECT COUNT(*) AS n
  FROM curated_ds.rejected_rides
  WHERE event_date BETWEEN @window_start AND @window_end AND reject_reason = 'TRIP_TOO_LONG'
),
checks AS (
  SELECT 'DQ-LT-01' AS check_id, 'no trips longer than 4 hours' AS check_name, 'ERROR' AS severity,
    CAST(too_long AS FLOAT64) AS observed_value, 0.0 AS threshold, too_long = 0 AS passed, 'rides with trip_minutes > 240' AS detail
  FROM trips
  UNION ALL
  SELECT 'DQ-LT-02', 'no negative trip minutes', 'ERROR', CAST(negative_minutes AS FLOAT64), 0.0, negative_minutes = 0,
    'rides with trip_minutes < 0'
  FROM trips
  UNION ALL
  SELECT 'DQ-LT-03', 'dropoff is not before pickup', 'ERROR', CAST(dropoff_before_pickup AS FLOAT64), 0.0, dropoff_before_pickup = 0,
    'rides with dropoff_ts < pickup_ts'
  FROM trips
  UNION ALL
  SELECT 'DQ-LT-04', 'trip_minutes agrees with the timestamps', 'ERROR', CAST(minutes_mismatch AS FLOAT64), 0.0, minutes_mismatch = 0,
    'rides where trip_minutes differs from dropoff - pickup by more than 0.01 minutes'
  FROM trips
  UNION ALL
  SELECT 'DQ-LT-05', 'zero-minute trip share', 'WARN', SAFE_DIVIDE(zero_minutes, n), max_zero_minute_share,
    COALESCE(SAFE_DIVIDE(zero_minutes, n), 0) <= max_zero_minute_share, 'rides with trip_minutes = 0'
  FROM trips
  UNION ALL
  SELECT 'DQ-LT-06', 'rides rejected as too long (context)', 'INFO', CAST(n AS FLOAT64), NULL, TRUE,
    'rows in curated_ds.rejected_rides with reason TRIP_TOO_LONG'
  FROM rejected_long
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
