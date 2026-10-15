-- Purpose: driver ranking by revenue over the latest 30 days of data, with each driver's current attributes.
-- Reads: curated_ds.fact_rides and the current rows of curated_ds.dim_driver_scd2.
-- Params: none (view)
-- Cost: scans only the last 30 pickup dates of the fact table (window anchored on the newest date that has driver data).
--   Creating the view is free; deploying it needs approval.
-- Notes: historical NYC rides have no driver_id and are excluded. The window ends at the newest pickup_date with a driver,
--   not at today, so the view still works when the stream is paused. current_* come from the current SCD2 version.

CREATE OR REPLACE VIEW mart_ds.v_driver_leaderboard AS
WITH bounds AS (
  SELECT MAX(pickup_date) AS window_end
  FROM curated_ds.fact_rides
  WHERE driver_id IS NOT NULL
),
agg AS (
  SELECT
    r.driver_id,
    COUNT(*) AS rides,
    SUM(r.fare) AS revenue,
    SAFE_DIVIDE(CAST(SUM(r.fare) AS FLOAT64), COUNT(*)) AS avg_fare,
    SUM(r.trip_minutes) / 60 AS driving_hours,
    COUNT(DISTINCT r.pickup_date) AS active_days,
    COUNTIF(r.surge_flag) AS surge_rides
  FROM curated_ds.fact_rides AS r, bounds AS b
  WHERE r.driver_id IS NOT NULL
    AND r.pickup_date BETWEEN DATE_SUB(b.window_end, INTERVAL 29 DAY) AND b.window_end
  GROUP BY r.driver_id
)
SELECT
  RANK() OVER (ORDER BY a.revenue DESC, a.driver_id) AS revenue_rank,
  a.driver_id,
  a.rides,
  a.revenue,
  a.avg_fare,
  a.driving_hours,
  a.active_days,
  a.surge_rides,
  d.vehicle_type AS current_vehicle_type,
  d.home_zone AS current_home_zone,
  d.status AS current_status,
  DATE_SUB(b.window_end, INTERVAL 29 DAY) AS window_start,
  b.window_end AS window_end
FROM agg AS a
CROSS JOIN bounds AS b
LEFT JOIN curated_ds.dim_driver_scd2 AS d
  ON d.driver_id = a.driver_id AND d.is_current;
