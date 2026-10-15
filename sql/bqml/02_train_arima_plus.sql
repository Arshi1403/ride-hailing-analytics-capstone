-- Purpose: step 2 of the forecast. Train one ARIMA_PLUS model with one time series per selected pickup zone.
-- Params: none (reads ml_ds.zone_hourly_training produced by step 1)
-- Cost: REQUIRES EXPLICIT APPROVAL. BQML training is billable (bytes processed by the training query, plus the BQML
--   model-creation rate for ARIMA_PLUS; check current BigQuery pricing). The input is small (selected zones x training hours),
--   but run `bq query --dry_run` first and set --maximum_bytes_billed. Training time grows with the number of series.
-- Overwrite note: CREATE OR REPLACE MODEL replaces ml_ds.zone_demand_arima, which is owned by this pipeline. Do not point it at
--   a model that someone else created.
-- Options (OPTIONS cannot take parameters; edit here):
--   data_frequency HOURLY, auto_arima TRUE, clean_spikes_and_dips TRUE (outlier hours do not distort the fit),
--   decompose_time_series TRUE (keeps trend/seasonality available via ML.EXPLAIN_FORECAST).
--   holiday_region is NOT set: hourly holiday handling was not verified. Add holiday_region = 'US' after checking the BQML docs.
-- Missing hours are handled in step 1 (zero-fill within citywide hours, gaps left out and interpolated by ARIMA_PLUS).

CREATE OR REPLACE MODEL ml_ds.zone_demand_arima
OPTIONS (
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'hour_ts',
  time_series_data_col = 'rides',
  time_series_id_col = 'pickup_zone',
  data_frequency = 'HOURLY',
  auto_arima = TRUE,
  clean_spikes_and_dips = TRUE,
  decompose_time_series = TRUE
) AS
SELECT
  hour_ts,
  pickup_zone,
  rides
FROM ml_ds.zone_hourly_training;
