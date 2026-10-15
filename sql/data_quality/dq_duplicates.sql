-- Purpose: duplicate checks: duplicate rides, SCD2 integrity (one current row per driver, no overlaps or gaps), raw duplicate share.
-- Params: @run_id STRING, @window_start DATE, @window_end DATE
-- Cost: REQUIRES APPROVAL for the first run. Fact check is partition-pruned to the window; the SCD2 checks read the small
--   dimension; the raw check is pruned to ingest dates window_start..window_end+7. Dry-run first.
-- Output: ops_ds.dq_results columns.
-- Note: at-least-once delivery makes duplicates in raw_ds.rides_stream normal; Spark removes them. That check only warns.
DECLARE max_raw_duplicate_share FLOAT64 DEFAULT 0.05;

WITH fact_dups AS (
  SELECT COUNT(*) AS n
  FROM (
    SELECT ride_id
    FROM curated_ds.fact_rides
    WHERE pickup_date BETWEEN @window_start AND @window_end
    GROUP BY ride_id
    HAVING COUNT(*) > 1
  )
),
per_driver AS (
  SELECT driver_id, COUNTIF(is_current) AS n_current
  FROM curated_ds.dim_driver_scd2
  GROUP BY driver_id
),
versions AS (
  SELECT
    driver_id,
    effective_start_ts,
    effective_end_ts,
    is_current,
    LEAD(effective_start_ts) OVER (PARTITION BY driver_id ORDER BY effective_start_ts) AS next_start
  FROM curated_ds.dim_driver_scd2
),
dup_versions AS (
  SELECT COUNT(*) AS n
  FROM (
    SELECT driver_id, effective_start_ts
    FROM curated_ds.dim_driver_scd2
    GROUP BY driver_id, effective_start_ts
    HAVING COUNT(*) > 1
  )
),
raw_dups AS (
  SELECT
    COUNT(*) AS n_rows,
    COUNT(DISTINCT ride_id) AS n_ids
  FROM raw_ds.rides_stream
  WHERE DATE(ingest_ts) BETWEEN @window_start AND DATE_ADD(@window_end, INTERVAL 7 DAY)
),
checks AS (
  SELECT 'DQ-DU-01' AS check_id, 'duplicate ride_id in curated rides' AS check_name, 'ERROR' AS severity,
    CAST(n AS FLOAT64) AS observed_value, 0.0 AS threshold, n = 0 AS passed, 'ride_id values that appear more than once' AS detail
  FROM fact_dups
  UNION ALL
  SELECT 'DQ-DU-02', 'every driver has exactly one current version', 'ERROR',
    CAST(COUNTIF(n_current != 1) AS FLOAT64), 0.0, COUNTIF(n_current != 1) = 0, 'drivers with zero or several current rows'
  FROM per_driver
  UNION ALL
  SELECT 'DQ-DU-03', 'SCD2 versions are contiguous (no overlap, no gap)', 'ERROR',
    CAST(COUNTIF(next_start IS NOT NULL AND effective_end_ts != next_start) AS FLOAT64), 0.0,
    COUNTIF(next_start IS NOT NULL AND effective_end_ts != next_start) = 0, 'versions whose end differs from the next start'
  FROM versions
  UNION ALL
  SELECT 'DQ-DU-04', 'no duplicate (driver_id, effective_start_ts)', 'ERROR',
    CAST(n AS FLOAT64), 0.0, n = 0, 'duplicated SCD2 versions'
  FROM dup_versions
  UNION ALL
  SELECT 'DQ-DU-05', 'is_current agrees with effective_end_ts', 'ERROR',
    CAST(COUNTIF(is_current != (effective_end_ts = TIMESTAMP '9999-12-31 00:00:00+00')) AS FLOAT64), 0.0,
    COUNTIF(is_current != (effective_end_ts = TIMESTAMP '9999-12-31 00:00:00+00')) = 0, 'versions with an inconsistent current flag'
  FROM versions
  UNION ALL
  SELECT 'DQ-DU-06', 'raw stream duplicate share', 'WARN',
    SAFE_DIVIDE(n_rows - n_ids, n_rows), max_raw_duplicate_share,
    COALESCE(SAFE_DIVIDE(n_rows - n_ids, n_rows), 0) <= max_raw_duplicate_share,
    'share of raw rows that repeat a ride_id (removed by Spark)'
  FROM raw_dups
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
