-- Purpose: fare sanity checks on curated rides, plus context on rides rejected for negative fares.
-- Params: @run_id STRING, @window_start DATE, @window_end DATE
-- Cost: REQUIRES APPROVAL for the first run. Reads fare/source columns of the window's partitions and the rejected table's window. Dry-run first.
-- Output: ops_ds.dq_results columns.
-- Rules: negative fares are rejected by Spark, so any negative fare in curated data is a pipeline bug (ERROR).
--   Zero fares are allowed by Spark but should be rare. Fares above 1000 exceed the streaming validation limit; historical
--   NYC data can contain outliers, so that check only warns.
DECLARE max_zero_fare_share FLOAT64 DEFAULT 0.01;
DECLARE max_high_fare_share FLOAT64 DEFAULT 0.001;

WITH fares AS (
  SELECT
    COUNT(*) AS n,
    COUNTIF(fare < 0) AS negative,
    COUNTIF(fare = 0) AS zero,
    COUNTIF(fare > 1000) AS high
  FROM curated_ds.fact_rides
  WHERE pickup_date BETWEEN @window_start AND @window_end
),
rejected_negative AS (
  SELECT COUNT(*) AS n
  FROM curated_ds.rejected_rides
  WHERE event_date BETWEEN @window_start AND @window_end AND reject_reason = 'NEGATIVE_FARE'
),
checks AS (
  SELECT 'DQ-NF-01' AS check_id, 'no negative fares in curated rides' AS check_name, 'ERROR' AS severity,
    CAST(negative AS FLOAT64) AS observed_value, 0.0 AS threshold, negative = 0 AS passed, 'rides with fare < 0' AS detail
  FROM fares
  UNION ALL
  SELECT 'DQ-NF-02', 'zero fare share', 'WARN',
    SAFE_DIVIDE(zero, n), max_zero_fare_share, COALESCE(SAFE_DIVIDE(zero, n), 0) <= max_zero_fare_share, 'rides with fare = 0'
  FROM fares
  UNION ALL
  SELECT 'DQ-NF-03', 'implausibly high fare share (> 1000)', 'WARN',
    SAFE_DIVIDE(high, n), max_high_fare_share, COALESCE(SAFE_DIVIDE(high, n), 0) <= max_high_fare_share, 'rides with fare > 1000'
  FROM fares
  UNION ALL
  SELECT 'DQ-NF-04', 'rides rejected for negative fares (context)', 'INFO',
    CAST(n AS FLOAT64), NULL, TRUE, 'rows in curated_ds.rejected_rides with reason NEGATIVE_FARE'
  FROM rejected_negative
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
