-- Purpose: row-count checks for one pipeline run: curated volume, day-over-day drop, raw-to-curated reconciliation, reject rate.
-- Params: @run_id STRING, @window_start DATE, @window_end DATE (the Spark pickup-date window), @business_tz STRING (the Spark --timezone)
-- Cost: REQUIRES APPROVAL for the first run. Scans curated rows for the window (partition-pruned) plus the raw stream
--   table for ingest dates window_start..window_end+7 (partition-pruned). Dry-run it and set --maximum_bytes_billed.
-- Output: ops_ds.dq_results columns (append with --destination_table ops_ds.dq_results --append_table).
--   status PASS | WARN | FAIL. Any FAIL (severity ERROR) must stop the pipeline before marts and forecast are refreshed.
-- Thresholds (edit here):
DECLARE max_recon_diff_pct FLOAT64 DEFAULT 0.005;  -- raw vs curated+rejected, stream rides
DECLARE min_volume_ratio FLOAT64 DEFAULT 0.5;      -- last day vs average of the 7 days before it
DECLARE max_reject_rate FLOAT64 DEFAULT 0.05;      -- rejected / (curated + rejected)

WITH curated AS (
  SELECT
    COUNT(*) AS n_all,
    COUNTIF(source = 'stream') AS n_stream
  FROM curated_ds.fact_rides
  WHERE pickup_date BETWEEN @window_start AND @window_end
),
rejected AS (
  SELECT
    COUNT(*) AS n_all,
    COUNT(DISTINCT IF(source = 'stream' AND ride_id IS NOT NULL AND pickup_ts IS NOT NULL, ride_id, NULL)) AS n_stream_ids
  FROM curated_ds.rejected_rides
  WHERE event_date BETWEEN @window_start AND @window_end
),
raw_stream AS (
  SELECT COUNT(DISTINCT ride_id) AS n_ids
  FROM raw_ds.rides_stream
  WHERE DATE(ingest_ts) BETWEEN @window_start AND DATE_ADD(@window_end, INTERVAL 7 DAY)
    AND DATE(pickup_ts, @business_tz) BETWEEN @window_start AND @window_end
    AND ride_id IS NOT NULL
),
daily AS (
  SELECT pickup_date, COUNT(*) AS rides
  FROM curated_ds.fact_rides
  WHERE pickup_date BETWEEN DATE_SUB(@window_end, INTERVAL 7 DAY) AND @window_end
  GROUP BY pickup_date
),
volume AS (
  SELECT
    (SELECT rides FROM daily WHERE pickup_date = @window_end) AS last_day,
    (SELECT AVG(rides) FROM daily WHERE pickup_date < @window_end) AS prev_avg
),
checks AS (
  SELECT 'DQ-RC-01' AS check_id, 'curated rows exist in the window' AS check_name, 'ERROR' AS severity,
    CAST(n_all AS FLOAT64) AS observed_value, 1.0 AS threshold, n_all > 0 AS passed,
    'rows in curated_ds.fact_rides for the window' AS detail
  FROM curated
  UNION ALL
  SELECT 'DQ-RC-02', 'last day volume vs previous 7-day average', 'WARN',
    SAFE_DIVIDE(last_day, prev_avg), min_volume_ratio,
    prev_avg IS NULL OR COALESCE(SAFE_DIVIDE(last_day, prev_avg), 0) >= min_volume_ratio,
    IF(prev_avg IS NULL, 'no history to compare with', 'ratio of window_end rides to the previous average')
  FROM volume
  UNION ALL
  SELECT 'DQ-RC-03', 'stream rides: raw distinct ride_id = curated + rejected', 'ERROR',
    SAFE_DIVIDE(ABS(r.n_ids - (c.n_stream + j.n_stream_ids)), NULLIF(r.n_ids, 0)), max_recon_diff_pct,
    (r.n_ids = 0 AND c.n_stream = 0)
      OR COALESCE(SAFE_DIVIDE(ABS(r.n_ids - (c.n_stream + j.n_stream_ids)), NULLIF(r.n_ids, 0)), 1) <= max_recon_diff_pct,
    CONCAT('raw=', CAST(r.n_ids AS STRING), ' curated=', CAST(c.n_stream AS STRING), ' rejected=', CAST(j.n_stream_ids AS STRING))
  FROM raw_stream AS r, curated AS c, rejected AS j
  UNION ALL
  SELECT 'DQ-RC-04', 'reject rate', 'ERROR',
    SAFE_DIVIDE(j.n_all, c.n_all + j.n_all), max_reject_rate,
    COALESCE(SAFE_DIVIDE(j.n_all, c.n_all + j.n_all), 0) <= max_reject_rate,
    CONCAT('rejected=', CAST(j.n_all AS STRING), ' curated=', CAST(c.n_all AS STRING))
  FROM curated AS c, rejected AS j
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
