-- Purpose: inspect the trained model: chosen ARIMA orders, seasonality and fit statistics per zone.
-- Params: none
-- Cost: metadata of the model; no table scan. Safe once the model exists. Approval needed only to query the project.
-- Use: sanity check after training (e.g. non-null aic, expected seasonal_periods such as DAILY and WEEKLY for hourly data).

SELECT *
FROM ML.ARIMA_EVALUATE(MODEL ml_ds.zone_demand_arima)
ORDER BY pickup_zone, aic;
