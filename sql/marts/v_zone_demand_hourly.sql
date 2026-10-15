-- Purpose: hourly ride demand per pickup zone. Actuals for the dashboard and the history the forecast is trained on.
-- Reads: curated_ds.fact_rides only.
-- Params: none (view)
-- Cost: aggregates the fact table; clustered by pickup_zone. Filter on pickup_date (partition) or hour_ts in the dashboard.
--   Creating the view is free; deploying it needs approval.
-- hour_ts is the start of the hour in UTC (TIMESTAMP_TRUNC of pickup_ts). pickup_date is the business date from Spark
--   (its --timezone) and can differ from DATE(hour_ts) when the timezone is not UTC.
-- Hours without rides have no row here; the forecast SQL (sql/bqml) zero-fills them where citywide data exists.

CREATE OR REPLACE VIEW mart_ds.v_zone_demand_hourly AS
SELECT
  pickup_zone,
  ANY_VALUE(pickup_zone_name) AS pickup_zone_name,
  ANY_VALUE(pickup_borough) AS pickup_borough,
  TIMESTAMP_TRUNC(pickup_ts, HOUR) AS hour_ts,
  MIN(pickup_date) AS pickup_date,
  COUNT(*) AS rides,
  SUM(fare) AS revenue,
  SAFE_DIVIDE(CAST(SUM(fare) AS FLOAT64), COUNT(*)) AS avg_fare,
  AVG(trip_minutes) AS avg_trip_minutes,
  COUNTIF(surge_flag) AS surge_rides
FROM curated_ds.fact_rides
WHERE pickup_zone IS NOT NULL
GROUP BY pickup_zone, hour_ts;
