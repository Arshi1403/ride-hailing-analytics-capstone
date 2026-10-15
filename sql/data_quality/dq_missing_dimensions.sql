-- Purpose: missing dimension matches: zones and drivers that could not be enriched by the Spark job.
-- Params: @run_id STRING, @window_start DATE, @window_end DATE
-- Cost: REQUIRES APPROVAL for the first run. Reads match-status columns of the window's partitions and counts the small dimension. Dry-run first.
-- Output: ops_ds.dq_results columns.
-- How to read the match columns (from the Spark job):
--   zone_match:   MATCHED | PICKUP_NOT_FOUND | DROPOFF_NOT_FOUND | BOTH_NOT_FOUND | DIMENSION_UNAVAILABLE
--   driver_match: MATCHED | NO_DRIVER_ID | DRIVER_NOT_FOUND | DIMENSION_UNAVAILABLE
--   Historical rides have no driver_id by design (NO_DRIVER_ID), so driver checks use stream rides only.
DECLARE zone_warn_share FLOAT64 DEFAULT 0.01;
DECLARE zone_error_share FLOAT64 DEFAULT 0.05;
DECLARE driver_warn_share FLOAT64 DEFAULT 0.02;
DECLARE driver_error_share FLOAT64 DEFAULT 0.10;

WITH rides AS (
  SELECT
    COUNT(*) AS n,
    COUNTIF(source = 'stream') AS n_stream,
    COUNTIF(source = 'historical') AS n_hist,
    COUNTIF(zone_match = 'DIMENSION_UNAVAILABLE') AS zone_unavailable,
    COUNTIF(zone_match IN ('PICKUP_NOT_FOUND', 'BOTH_NOT_FOUND')) AS pickup_missing,
    COUNTIF(zone_match IN ('DROPOFF_NOT_FOUND', 'BOTH_NOT_FOUND')) AS dropoff_missing,
    COUNTIF(source = 'stream' AND driver_match IN ('DRIVER_NOT_FOUND', 'NO_DRIVER_ID')) AS stream_driver_missing,
    COUNTIF(source = 'stream' AND driver_match = 'DIMENSION_UNAVAILABLE') AS stream_driver_unavailable,
    COUNTIF(source = 'historical' AND driver_match = 'NO_DRIVER_ID') AS hist_no_driver
  FROM curated_ds.fact_rides
  WHERE pickup_date BETWEEN @window_start AND @window_end
),
dim AS (
  SELECT COUNTIF(is_current) AS current_drivers FROM curated_ds.dim_driver_scd2
),
checks AS (
  SELECT 'DQ-MD-01' AS check_id, 'zone dimension was available' AS check_name, 'ERROR' AS severity,
    CAST(zone_unavailable AS FLOAT64) AS observed_value, 0.0 AS threshold, zone_unavailable = 0 AS passed,
    'rides with zone_match = DIMENSION_UNAVAILABLE' AS detail
  FROM rides
  UNION ALL
  SELECT 'DQ-MD-02', 'pickup zone match rate (warn level)', 'WARN', SAFE_DIVIDE(pickup_missing, n), zone_warn_share,
    COALESCE(SAFE_DIVIDE(pickup_missing, n), 0) <= zone_warn_share, 'share of rides whose pickup zone is not in the zone dimension'
  FROM rides
  UNION ALL
  SELECT 'DQ-MD-03', 'pickup zone match rate (error level)', 'ERROR', SAFE_DIVIDE(pickup_missing, n), zone_error_share,
    COALESCE(SAFE_DIVIDE(pickup_missing, n), 0) <= zone_error_share, 'same measure, blocking threshold'
  FROM rides
  UNION ALL
  SELECT 'DQ-MD-04', 'dropoff zone match rate', 'WARN', SAFE_DIVIDE(dropoff_missing, n), zone_warn_share,
    COALESCE(SAFE_DIVIDE(dropoff_missing, n), 0) <= zone_warn_share, 'share of rides whose dropoff zone is not in the zone dimension'
  FROM rides
  UNION ALL
  SELECT 'DQ-MD-05', 'stream rides: driver dimension was available', 'ERROR', CAST(stream_driver_unavailable AS FLOAT64), 0.0,
    stream_driver_unavailable = 0, 'stream rides with driver_match = DIMENSION_UNAVAILABLE'
  FROM rides
  UNION ALL
  SELECT 'DQ-MD-06', 'stream rides: driver match rate (warn level)', 'WARN', SAFE_DIVIDE(stream_driver_missing, n_stream), driver_warn_share,
    COALESCE(SAFE_DIVIDE(stream_driver_missing, n_stream), 0) <= driver_warn_share, 'stream rides without a matching driver version'
  FROM rides
  UNION ALL
  SELECT 'DQ-MD-07', 'stream rides: driver match rate (error level)', 'ERROR', SAFE_DIVIDE(stream_driver_missing, n_stream), driver_error_share,
    COALESCE(SAFE_DIVIDE(stream_driver_missing, n_stream), 0) <= driver_error_share, 'same measure, blocking threshold'
  FROM rides
  UNION ALL
  SELECT 'DQ-MD-08', 'historical rides without driver (context)', 'INFO', CAST(hist_no_driver AS FLOAT64), NULL, TRUE,
    'expected: the NYC TLC data has no driver id'
  FROM rides
  UNION ALL
  SELECT 'DQ-MD-09', 'driver dimension has current rows when stream rides exist', 'ERROR', CAST(current_drivers AS FLOAT64), 1.0,
    current_drivers > 0 OR n_stream = 0, 'current versions in curated_ds.dim_driver_scd2'
  FROM dim, rides
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
