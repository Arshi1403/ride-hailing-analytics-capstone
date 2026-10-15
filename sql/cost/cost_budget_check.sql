-- Purpose: month-to-date spend against a budget, with PASS / WARN (80 percent) / FAIL (100 percent).
-- Params: @budget_usd FLOAT64, @price_per_tib FLOAT64 (on-demand price per TiB; not hardcoded)
-- Source: region-asia-south1.INFORMATION_SCHEMA.JOBS_BY_PROJECT (edit the region qualifier if BQ_LOCATION differs). Needs bigquery.jobs.listAll.
-- Cost: tiny (job metadata), approval still required to run.
-- Use: Composer can run this before billable steps (training, backfills) and stop on FAIL. Same counting rules and the same
--   estimate caveats as cost_daily_bytes_billed.sql; the month is the current UTC month.

WITH month_to_date AS (
  SELECT
    COALESCE(SUM(total_bytes_billed), 0) AS bytes_billed,
    COUNT(*) AS jobs
  FROM `region-asia-south1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE creation_time >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)
    AND job_type = 'QUERY'
    AND state = 'DONE'
    AND error_result IS NULL
    AND reservation_id IS NULL
    AND (statement_type IS NULL OR statement_type != 'SCRIPT')
)
SELECT
  DATE_TRUNC(CURRENT_DATE(), MONTH) AS month_start,
  jobs,
  bytes_billed,
  SAFE_DIVIDE(bytes_billed, POW(1024, 4)) * @price_per_tib AS estimated_cost_usd,
  @budget_usd AS budget_usd,
  SAFE_DIVIDE(SAFE_DIVIDE(bytes_billed, POW(1024, 4)) * @price_per_tib, @budget_usd) AS budget_used_share,
  CASE
    WHEN SAFE_DIVIDE(bytes_billed, POW(1024, 4)) * @price_per_tib >= @budget_usd THEN 'FAIL'
    WHEN SAFE_DIVIDE(bytes_billed, POW(1024, 4)) * @price_per_tib >= 0.8 * @budget_usd THEN 'WARN'
    ELSE 'PASS'
  END AS status
FROM month_to_date;
