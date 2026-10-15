-- Purpose: step 1 of the forecast. Pick the top zones and build the dense hourly training series.
-- Params: @as_of_ts TIMESTAMP (hour-aligned UTC, exclusive end of history = first forecast hour),
--         @train_days INT64 (history length, e.g. 56), @top_n INT64 (1..10, normally 10),
--         @min_rides INT64 (low-volume cut-off, e.g. 500), @min_active_hours INT64 (e.g. 168), @run_id STRING
-- Cost: REQUIRES APPROVAL. Reads mart_ds.v_zone_demand_hourly (the capstone's training source), which aggregates curated_ds.fact_rides;
--   BigQuery cannot prune fact partitions through that view's GROUP BY, so expect a scan of the narrow columns the view uses.
--   Dry-run first and keep --maximum_bytes_billed (10 GB). Creates/replaces ml_ds.zone_hourly_training (a derived table owned by this pipeline).
--   Create the view first (sql/marts/v_zone_demand_hourly.sql).
-- Why an explicit as_of_ts: the historical subset is NYC 2018 and the stream is current, so "now" may have no history.
--   Choose as_of_ts at the end of the data you want to forecast from; the model then predicts the next 168 hours.
--
-- Low-volume zones: a zone is eligible only when rides >= @min_rides AND active_hours >= @min_active_hours in the window.
--   Ineligible zones are written to ml_ds.zone_forecast_eligibility with a reason and are never trained; the forecast then
--   has fewer than @top_n zones. Rank = rides in the window, ties broken by zone id.
-- Missing hours:
--   * Hours where NO zone had a ride (outage or no data) are treated as data gaps: they are left out of the series and
--     ARIMA_PLUS interpolates them.
--   * Hours with citywide rides but none in a selected zone are real zero demand and are filled with 0.

DECLARE train_start TIMESTAMP DEFAULT TIMESTAMP_SUB(@as_of_ts, INTERVAL @train_days * 24 HOUR);

ASSERT TIMESTAMP_TRUNC(@as_of_ts, HOUR) = @as_of_ts AS '@as_of_ts must be aligned to the hour';
ASSERT @train_days BETWEEN 14 AND 365 AS '@train_days must be between 14 and 365';
ASSERT @top_n BETWEEN 1 AND 10 AS '@top_n must be between 1 and 10';
ASSERT @min_rides >= 1 AND @min_active_hours >= 1 AS 'thresholds must be positive';

CREATE TEMP TABLE zone_hours AS
SELECT
  pickup_zone,
  pickup_zone_name,
  pickup_borough,
  hour_ts,
  rides
FROM mart_ds.v_zone_demand_hourly
WHERE hour_ts >= train_start
  AND hour_ts < @as_of_ts;

ASSERT (SELECT COUNT(*) FROM zone_hours) > 0
  AS 'no rides in the training window: check @as_of_ts and @train_days';

CREATE TEMP TABLE zone_stats AS
SELECT
  pickup_zone,
  ANY_VALUE(pickup_zone_name) AS pickup_zone_name,
  ANY_VALUE(pickup_borough) AS pickup_borough,
  SUM(rides) AS rides,
  COUNT(*) AS active_hours,
  SUM(rides) >= @min_rides AND COUNT(*) >= @min_active_hours AS eligible,
  CASE
    WHEN SUM(rides) < @min_rides THEN 'LOW_VOLUME'
    WHEN COUNT(*) < @min_active_hours THEN 'TOO_FEW_ACTIVE_HOURS'
  END AS reason
FROM zone_hours
GROUP BY pickup_zone;

CREATE TEMP TABLE ranked AS
SELECT
  s.*,
  IF(eligible, ROW_NUMBER() OVER (PARTITION BY eligible ORDER BY rides DESC, pickup_zone), NULL) AS zone_rank
FROM zone_stats AS s;

-- Delete-then-insert for this as_of_ts. Not wrapped in a transaction (it reads temp tables); a failed run is repaired by rerunning.
DELETE FROM ml_ds.zone_forecast_eligibility WHERE as_of_ts = @as_of_ts;

INSERT INTO ml_ds.zone_forecast_eligibility (
  as_of_ts, pickup_zone, pickup_zone_name, pickup_borough, rides, active_hours, eligible, zone_rank, selected, reason, run_id, created_ts
)
SELECT
  @as_of_ts, pickup_zone, pickup_zone_name, pickup_borough, rides, active_hours, eligible, zone_rank,
  eligible AND zone_rank <= @top_n AS selected, reason, @run_id, CURRENT_TIMESTAMP()
FROM ranked;

CREATE OR REPLACE TABLE ml_ds.zone_hourly_training
PARTITION BY DATE(hour_ts)
CLUSTER BY pickup_zone
OPTIONS (description = 'Dense hourly ride counts for the selected zones; input of ml_ds.zone_demand_arima. Replaced on every run.')
AS
WITH selected_zones AS (
  SELECT pickup_zone FROM ranked WHERE eligible AND zone_rank <= @top_n
),
city_hours AS (
  SELECT hour_ts FROM zone_hours GROUP BY hour_ts HAVING SUM(rides) > 0
)
SELECT
  @as_of_ts AS as_of_ts,
  z.pickup_zone,
  h.hour_ts,
  COALESCE(zh.rides, 0) AS rides
FROM selected_zones AS z
CROSS JOIN city_hours AS h
LEFT JOIN zone_hours AS zh
  ON zh.pickup_zone = z.pickup_zone AND zh.hour_ts = h.hour_ts;
