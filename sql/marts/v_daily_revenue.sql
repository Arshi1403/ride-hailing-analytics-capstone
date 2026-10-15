-- Purpose: daily revenue and ride KPIs per pickup date and data source, for the dashboard.
-- Reads: curated_ds.fact_rides only (validated rows; never raw).
-- Params: none (view)
-- Cost: aggregates the whole fact table (narrow column set). Dashboards should cache or use a date filter; a date filter
--   on pickup_date prunes partitions. Creating the view is free and needs approval only as a deployment step.
-- Columns:
--   pickup_date, source ('stream' | 'historical'), rides, revenue (sum of fare), avg_fare, total_trip_minutes, avg_trip_minutes,
--   surge_rides, surge_ride_share (over rides with a known surge value; null for historical-only days),
--   revenue_per_km (only rides with a valid distance), active_drivers (null drivers are not counted), last_processing_date.

CREATE OR REPLACE VIEW mart_ds.v_daily_revenue AS
SELECT
  pickup_date,
  source,
  COUNT(*) AS rides,
  SUM(fare) AS revenue,
  SAFE_DIVIDE(CAST(SUM(fare) AS FLOAT64), COUNT(*)) AS avg_fare,
  SUM(trip_minutes) AS total_trip_minutes,
  AVG(trip_minutes) AS avg_trip_minutes,
  COUNTIF(surge_flag) AS surge_rides,
  SAFE_DIVIDE(COUNTIF(surge_flag), COUNTIF(surge_flag IS NOT NULL)) AS surge_ride_share,
  SAFE_DIVIDE(
    SUM(IF(distance_km > 0, CAST(fare AS FLOAT64), NULL)),
    SUM(IF(distance_km > 0, distance_km, NULL))
  ) AS revenue_per_km,
  COUNT(DISTINCT driver_id) AS active_drivers,
  MAX(processing_date) AS last_processing_date
FROM curated_ds.fact_rides
GROUP BY pickup_date, source;
