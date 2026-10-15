-- Purpose: step 3 of the forecast. Write 168 hourly predictions per selected zone to ml_ds.zone_demand_forecast.
-- Params: @as_of_ts TIMESTAMP (same value used in step 1), @run_id STRING, @confidence_level FLOAT64 (e.g. 0.9)
-- Cost: REQUIRES APPROVAL. ML.FORECAST reads the trained model (no table scan). Run with --maximum_bytes_billed anyway.
-- Rerun safety: delete-and-insert for this as_of_ts in one transaction. Earlier as_of_ts forecasts are kept for back-testing.
-- Output for dashboards: as_of_ts, forecast_ts (UTC hour start), horizon_hour 1..168, pickup_zone, pickup_zone_name,
--   pickup_borough, zone_rank, predicted_rides, lower_bound, upper_bound (all floored at 0), confidence_level.
-- Horizon: 168 hours = 7 days. horizon_hour counts forecast steps per zone (1 = first hour after the series ends).
--   sql/bqml/04_validate_forecast.sql checks that the first forecast hour equals @as_of_ts.

ASSERT (SELECT COUNT(*) FROM ml_ds.zone_hourly_training WHERE as_of_ts = @as_of_ts) > 0
  AS 'ml_ds.zone_hourly_training does not match @as_of_ts: rerun step 1 and step 2 first';
ASSERT @confidence_level > 0 AND @confidence_level < 1 AS '@confidence_level must be between 0 and 1';

BEGIN TRANSACTION;

DELETE FROM ml_ds.zone_demand_forecast WHERE as_of_ts = @as_of_ts;

INSERT INTO ml_ds.zone_demand_forecast (
  as_of_ts, forecast_ts, horizon_hour, pickup_zone, pickup_zone_name, pickup_borough, zone_rank,
  predicted_rides, lower_bound, upper_bound, confidence_level, model_name, run_id, generated_ts
)
SELECT
  @as_of_ts AS as_of_ts,
  f.forecast_timestamp AS forecast_ts,
  ROW_NUMBER() OVER (PARTITION BY f.pickup_zone ORDER BY f.forecast_timestamp) AS horizon_hour,
  f.pickup_zone,
  e.pickup_zone_name,
  e.pickup_borough,
  e.zone_rank,
  GREATEST(f.forecast_value, 0) AS predicted_rides,
  GREATEST(f.prediction_interval_lower_bound, 0) AS lower_bound,
  GREATEST(f.prediction_interval_upper_bound, 0) AS upper_bound,
  f.confidence_level,
  'ml_ds.zone_demand_arima' AS model_name,
  @run_id AS run_id,
  CURRENT_TIMESTAMP() AS generated_ts
FROM ML.FORECAST(
  MODEL ml_ds.zone_demand_arima,
  STRUCT(168 AS horizon, @confidence_level AS confidence_level)
) AS f
JOIN ml_ds.zone_forecast_eligibility AS e
  ON e.as_of_ts = @as_of_ts AND e.pickup_zone = f.pickup_zone AND e.selected;

COMMIT TRANSACTION;
