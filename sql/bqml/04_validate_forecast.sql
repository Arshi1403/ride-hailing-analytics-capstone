-- Purpose: step 4 of the forecast. Validate the forecast and its training series; returns data-quality rows.
-- Params: @as_of_ts TIMESTAMP, @top_n INT64, @run_id STRING
-- Cost: reads the small ml_ds tables only. Needs approval only because it queries the project; no model training happens here.
-- Output columns match ops_ds.dq_results, so the result can be appended with --destination_table ops_ds.dq_results --append_table.
--   status: PASS, WARN (severity WARN) or FAIL (severity ERROR). Any FAIL means the forecast must not be published.

WITH selected AS (
  SELECT pickup_zone FROM ml_ds.zone_forecast_eligibility WHERE as_of_ts = @as_of_ts AND selected
),
per_zone AS (
  SELECT
    s.pickup_zone,
    COUNT(f.forecast_ts) AS n_rows,
    MIN(f.forecast_ts) AS first_ts,
    MAX(f.forecast_ts) AS last_ts
  FROM selected AS s
  LEFT JOIN ml_ds.zone_demand_forecast AS f
    ON f.as_of_ts = @as_of_ts AND f.pickup_zone = s.pickup_zone
  GROUP BY s.pickup_zone
),
training AS (
  SELECT
    COUNT(DISTINCT hour_ts) AS hours_with_data,
    TIMESTAMP_DIFF(@as_of_ts, MIN(hour_ts), HOUR) AS window_hours,
    MAX(hour_ts) AS last_hour
  FROM ml_ds.zone_hourly_training
  WHERE as_of_ts = @as_of_ts
),
checks AS (
  SELECT 'FC-01' AS check_id, 'selected zone count within 1..top_n' AS check_name, 'ERROR' AS severity,
    CAST((SELECT COUNT(*) FROM selected) AS FLOAT64) AS observed_value, CAST(@top_n AS FLOAT64) AS threshold,
    (SELECT COUNT(*) FROM selected) BETWEEN 1 AND @top_n AS passed, 'zones selected for the forecast' AS detail
  UNION ALL
  SELECT 'FC-02', 'every selected zone has exactly 168 forecast rows', 'ERROR',
    CAST(COUNTIF(n_rows != 168) AS FLOAT64), 0.0, COUNTIF(n_rows != 168) = 0, 'zones with a wrong row count'
  FROM per_zone
  UNION ALL
  SELECT 'FC-03', 'first forecast hour equals as_of_ts', 'ERROR',
    CAST(COUNTIF(first_ts != @as_of_ts) AS FLOAT64), 0.0, COUNTIF(first_ts != @as_of_ts) = 0,
    'zones whose forecast does not start at as_of_ts (the training series ended earlier)'
  FROM per_zone
  UNION ALL
  SELECT 'FC-04', 'forecast hours are contiguous (168 hourly steps)', 'ERROR',
    CAST(COUNTIF(TIMESTAMP_DIFF(last_ts, first_ts, HOUR) != 167) AS FLOAT64), 0.0,
    COUNTIF(TIMESTAMP_DIFF(last_ts, first_ts, HOUR) != 167) = 0, 'zones with gaps or extra hours'
  FROM per_zone
  UNION ALL
  SELECT 'FC-05', 'predictions are non-negative and lower <= predicted <= upper', 'ERROR',
    CAST(COUNTIF(predicted_rides < 0 OR lower_bound > predicted_rides OR predicted_rides > upper_bound) AS FLOAT64), 0.0,
    COUNTIF(predicted_rides < 0 OR lower_bound > predicted_rides OR predicted_rides > upper_bound) = 0, 'invalid prediction rows'
  FROM ml_ds.zone_demand_forecast
  WHERE as_of_ts = @as_of_ts
  UNION ALL
  SELECT 'FC-06', 'timestamps and zone ids are present', 'ERROR',
    CAST(COUNTIF(forecast_ts IS NULL OR pickup_zone IS NULL OR as_of_ts IS NULL) AS FLOAT64), 0.0,
    COUNTIF(forecast_ts IS NULL OR pickup_zone IS NULL OR as_of_ts IS NULL) = 0, 'rows missing forecast_ts, pickup_zone or as_of_ts'
  FROM ml_ds.zone_demand_forecast
  WHERE as_of_ts = @as_of_ts
  UNION ALL
  SELECT 'FC-07', 'training series covers at least 90% of the window hours', 'WARN',
    SAFE_DIVIDE(hours_with_data, window_hours), 0.9, SAFE_DIVIDE(hours_with_data, window_hours) >= 0.9,
    'share of hours with citywide data; missing hours are interpolated by ARIMA_PLUS'
  FROM training
  UNION ALL
  SELECT 'FC-08', 'training series ends at the hour before as_of_ts', 'WARN',
    CAST(TIMESTAMP_DIFF(@as_of_ts, last_hour, HOUR) AS FLOAT64), 1.0, last_hour = TIMESTAMP_SUB(@as_of_ts, INTERVAL 1 HOUR),
    'hours between the last training hour and as_of_ts (should be 1)'
  FROM training
)
SELECT
  @run_id AS run_id,
  DATE(@as_of_ts) AS window_start,
  DATE_ADD(DATE(@as_of_ts), INTERVAL 7 DAY) AS window_end,
  check_id,
  check_name,
  severity,
  IF(passed, 'PASS', IF(severity = 'ERROR', 'FAIL', 'WARN')) AS status,
  observed_value,
  threshold,
  detail,
  CURRENT_TIMESTAMP() AS checked_ts
FROM checks
ORDER BY check_id;
