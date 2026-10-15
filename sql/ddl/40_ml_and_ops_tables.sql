-- Purpose: forecast result tables and the data-quality results table.
-- Params: none
-- Cost: metadata only. Approval needed (creates tables).
-- Written by: sql/bqml/*.sql (ml_ds tables) and by appending the output of sql/data_quality/*.sql (ops_ds.dq_results).

-- Which zones were considered for forecasting and why a zone was or was not eligible (low-volume handling).
CREATE TABLE IF NOT EXISTS ml_ds.zone_forecast_eligibility (
  as_of_ts TIMESTAMP NOT NULL OPTIONS (description = 'Exclusive end of the training series; first forecast hour'),
  pickup_zone STRING NOT NULL,
  pickup_zone_name STRING,
  pickup_borough STRING,
  rides INT64 NOT NULL OPTIONS (description = 'Rides in the training window'),
  active_hours INT64 NOT NULL OPTIONS (description = 'Hours with at least one ride in the training window'),
  eligible BOOL NOT NULL,
  zone_rank INT64 OPTIONS (description = 'Rank by rides among eligible zones; null if not eligible'),
  selected BOOL NOT NULL OPTIONS (description = 'True when eligible and rank <= top_n'),
  reason STRING OPTIONS (description = 'Why a zone is not eligible: LOW_VOLUME, TOO_FEW_ACTIVE_HOURS'),
  run_id STRING NOT NULL,
  created_ts TIMESTAMP NOT NULL
)
PARTITION BY DATE(as_of_ts)
CLUSTER BY pickup_zone
OPTIONS (description = 'Forecast zone selection per as_of_ts. Rewritten per as_of_ts by sql/bqml/01_select_zones_and_series.sql.');

-- Dashboard-facing predictions: one row per zone and forecast hour, with timestamps and zone identifiers.
CREATE TABLE IF NOT EXISTS ml_ds.zone_demand_forecast (
  as_of_ts TIMESTAMP NOT NULL OPTIONS (description = 'Forecast origin: history ends just before this hour (UTC)'),
  forecast_ts TIMESTAMP NOT NULL OPTIONS (description = 'Start of the predicted hour, UTC'),
  horizon_hour INT64 NOT NULL OPTIONS (description = '1 to 168'),
  pickup_zone STRING NOT NULL OPTIONS (description = 'NYC TLC zone id'),
  pickup_zone_name STRING,
  pickup_borough STRING,
  zone_rank INT64 NOT NULL OPTIONS (description = 'Rank by training-window rides, 1 = busiest'),
  predicted_rides FLOAT64 NOT NULL OPTIONS (description = 'Forecast demand, floored at 0'),
  lower_bound FLOAT64 NOT NULL,
  upper_bound FLOAT64 NOT NULL,
  confidence_level FLOAT64 NOT NULL,
  model_name STRING NOT NULL,
  run_id STRING NOT NULL,
  generated_ts TIMESTAMP NOT NULL
)
PARTITION BY DATE(as_of_ts)
CLUSTER BY pickup_zone
OPTIONS (description = 'Hourly demand forecast for the top zones. Previous as_of_ts values are kept for back-testing.');

CREATE TABLE IF NOT EXISTS ops_ds.dq_results (
  run_id STRING NOT NULL,
  window_start DATE NOT NULL,
  window_end DATE NOT NULL,
  check_id STRING NOT NULL,
  check_name STRING NOT NULL,
  severity STRING NOT NULL OPTIONS (description = 'ERROR blocks the pipeline, WARN is reported, INFO is context'),
  status STRING NOT NULL OPTIONS (description = 'PASS, WARN or FAIL (FAIL only for ERROR severity)'),
  observed_value FLOAT64,
  threshold FLOAT64,
  detail STRING,
  checked_ts TIMESTAMP NOT NULL
)
PARTITION BY DATE(checked_ts)
CLUSTER BY check_id
OPTIONS (description = 'Data-quality results. Append the output of sql/data_quality/*.sql; the column order matches those queries.');
