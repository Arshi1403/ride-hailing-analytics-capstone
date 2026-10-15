-- Purpose: the most expensive queries in the look-back period, to find what to optimise.
-- Params: @days INT64, @top_n INT64 (e.g. 20), @price_per_tib FLOAT64 (on-demand price per TiB; not hardcoded)
-- Source: region-asia-south1.INFORMATION_SCHEMA.JOBS_BY_PROJECT (edit the region qualifier if BQ_LOCATION differs).
-- Cost: tiny (job metadata), approval still required to run. Needs bigquery.jobs.listAll.
-- Notes: query_text is cut to 500 characters. SCRIPT parent jobs are excluded; their child statements are listed instead.
--   Look for: full scans of curated_ds.fact_rides without a pickup_date filter, repeated dashboard queries (cache_hit = FALSE),
--   and CREATE_MODEL jobs.

SELECT
  job_id,
  creation_time,
  user_email,
  statement_type,
  destination_table.dataset_id AS destination_dataset,
  total_bytes_billed AS bytes_billed,
  SAFE_DIVIDE(total_bytes_billed, POW(1024, 3)) AS gib_billed,
  SAFE_DIVIDE(total_bytes_billed, POW(1024, 4)) * @price_per_tib AS estimated_cost_usd,
  total_slot_ms,
  cache_hit,
  SUBSTR(query, 1, 500) AS query_text
FROM `region-asia-south1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
  AND job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
  AND reservation_id IS NULL
  AND (statement_type IS NULL OR statement_type != 'SCRIPT')
ORDER BY total_bytes_billed DESC
LIMIT @top_n;
