-- Purpose: daily BigQuery on-demand usage: bytes billed, TiB, estimated cost, and how much came from BQML model training.
-- Params: @days INT64 (look-back, e.g. 30), @price_per_tib FLOAT64 (on-demand price per TiB; check current pricing, it is not hardcoded)
-- Source: region-asia-south1.INFORMATION_SCHEMA.JOBS_BY_PROJECT. Change the region qualifier if BQ_LOCATION differs
--   (the qualifier cannot be a parameter). Needs bigquery.jobs.listAll (e.g. roles/bigquery.resourceViewer).
-- Cost: tiny (job metadata, filtered by creation_time which is the partition column), but approval is still required to run.
-- Counting rules: query jobs that finished without error and ran on demand (reservation_id IS NULL). Child jobs of a script are
--   counted and the parent SCRIPT job is excluded, so scripts are not double counted.
-- Estimated cost is bytes_billed / 2^40 * @price_per_tib. It ignores free-tier credits, reservations and BQML pricing that is not
--   bytes based; compare with the Cloud Billing report before reporting a number.

SELECT
  DATE(creation_time) AS usage_date,
  COUNT(*) AS jobs,
  SUM(total_bytes_billed) AS bytes_billed,
  SAFE_DIVIDE(SUM(total_bytes_billed), POW(1024, 4)) AS tib_billed,
  SAFE_DIVIDE(SUM(total_bytes_billed), POW(1024, 4)) * @price_per_tib AS estimated_cost_usd,
  SUM(IF(statement_type = 'CREATE_MODEL', total_bytes_billed, 0)) AS model_training_bytes_billed,
  SUM(total_slot_ms) / 1000 AS slot_seconds,
  COUNTIF(cache_hit) AS cache_hits
FROM `region-asia-south1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
  AND job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
  AND reservation_id IS NULL
  AND (statement_type IS NULL OR statement_type != 'SCRIPT')
GROUP BY usage_date
ORDER BY usage_date DESC;
