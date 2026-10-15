-- Purpose: dashboard-facing hourly demand forecast (latest forecast run) with the actual rides once they are known.
-- Reads: ml_ds.zone_demand_forecast and curated_ds.fact_rides (only the forecast dates).
-- Params: none (view)
-- Cost: reads the forecast table (small) and the fact table for the 8-9 days around the latest as_of_ts.
--   Creating the view is free; deploying it needs approval.
-- Every row has forecast_ts (UTC hour start) and pickup_zone (+ name, borough).
-- actual_rides is null when the hour is not observed yet or the zone had no rides in that hour (no fact rows).

CREATE OR REPLACE VIEW mart_ds.v_zone_demand_forecast AS
WITH latest AS (
  SELECT MAX(as_of_ts) AS as_of_ts FROM ml_ds.zone_demand_forecast
),
forecast AS (
  SELECT f.*
  FROM ml_ds.zone_demand_forecast AS f
  JOIN latest AS l ON f.as_of_ts = l.as_of_ts
),
actual AS (
  SELECT
    pickup_zone,
    TIMESTAMP_TRUNC(pickup_ts, HOUR) AS hour_ts,
    COUNT(*) AS rides
  FROM curated_ds.fact_rides
  WHERE pickup_date BETWEEN DATE_SUB((SELECT DATE(as_of_ts) FROM latest), INTERVAL 1 DAY)
                        AND DATE_ADD((SELECT DATE(as_of_ts) FROM latest), INTERVAL 9 DAY)
    AND pickup_zone IN (SELECT pickup_zone FROM forecast)
  GROUP BY pickup_zone, hour_ts
)
SELECT
  f.as_of_ts,
  f.forecast_ts,
  f.horizon_hour,
  f.pickup_zone,
  f.pickup_zone_name,
  f.pickup_borough,
  f.zone_rank,
  f.predicted_rides,
  f.lower_bound,
  f.upper_bound,
  f.confidence_level,
  a.rides AS actual_rides,
  f.model_name,
  f.generated_ts
FROM forecast AS f
LEFT JOIN actual AS a
  ON a.pickup_zone = f.pickup_zone AND a.hour_ts = f.forecast_ts;
