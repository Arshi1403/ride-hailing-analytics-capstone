# Contract 03: BigQuery Schemas (v0.1.0, draft)

Affected agents: ingestion, spark, bigquery, composer, testing. All datasets in `BQ_LOCATION`. DDL uses `CREATE TABLE IF NOT EXISTS`, never `CREATE OR REPLACE` on data tables.

## rh_raw (append-only, never read by marts)
| Table | Columns | Partition / cluster |
|---|---|---|
| ride_events_raw | event_id, event_type, event_version, event_ts, ride_id, rider_id, driver_id, pickup_zone_id, dropoff_zone_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, fare_amount NUMERIC, currency, distance_km, payment_method, vehicle_type, surge_multiplier, cancellation_reason, pubsub_message_id, pubsub_publish_ts, ingest_ts | DATE(event_ts) / event_type, pickup_zone_id |
| driver_events_raw | event_id, event_type, event_version, event_ts, driver_id, city, vehicle_type, status, home_zone_id, rating FLOAT64, pubsub_message_id, pubsub_publish_ts, ingest_ts | DATE(event_ts) / driver_id |
| rejected_events | raw_payload, error_code, error_detail, source, ingest_ts | DATE(ingest_ts) |
| hist_trips_raw | ride_id, rider_id, driver_id, pickup_zone_id, dropoff_zone_id, requested_ts, started_ts, completed_ts, status, fare_amount NUMERIC, currency, distance_km, payment_method, vehicle_type, surge_multiplier, cancellation_reason, source_file, batch_id, ingest_ts | DATE(requested_ts) / pickup_zone_id |
| hist_drivers_raw | driver_id, city, vehicle_type, status, home_zone_id, rating FLOAT64, effective_ts, source_file, batch_id, ingest_ts | DATE(effective_ts) |
| zones_raw | zone_id, zone_name, city, centroid_lat, centroid_lng, batch_id, ingest_ts | none |

Types not listed above follow contract 01 (STRING, FLOAT64, TIMESTAMP).

## rh_curated
| Table | Columns | Key / partition / cluster |
|---|---|---|
| fact_rides | ride_id, rider_id, driver_id, pickup_zone_id, dropoff_zone_id, ride_date DATE, requested_ts, assigned_ts, started_ts, completed_ts, cancelled_ts, status (`COMPLETED`, `CANCELLED`, `OPEN`), wait_seconds INT64, trip_seconds INT64, fare_amount NUMERIC, currency, distance_km, surge_multiplier, payment_method, vehicle_type, cancellation_reason, source, last_event_ts, processed_ts, run_id | key ride_id / ride_date / pickup_zone_id, driver_id |
| dim_driver_scd2 | driver_sk STRING (`TO_HEX(SHA256(driver_id \|\| '\|' \|\| effective_from_iso))`), driver_id, city, vehicle_type, status, home_zone_id, rating_band, record_hash, effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOL, processed_ts, run_id | key driver_sk / DATE(effective_from) / driver_id |
| dim_zone | zone_id, zone_name, city, centroid_lat, centroid_lng, processed_ts, run_id | key zone_id |
| rejected_records | record_type (`RIDE`, `DRIVER`), record_key, rule_id, rule_description, payload STRING, source, run_id, run_date DATE, rejected_ts | run_date |

SCD2 invariants (checked by DQ): exactly one `is_current` row per driver; intervals do not overlap; `effective_to` of a closed row equals `effective_from` of the next; open rows end `9999-12-31 23:59:59 UTC`.

## rh_staging (truncated and loaded from Parquet each run; same columns as curated plus)
`fact_rides_stg`, `dim_zone_stg`, `rejected_records_stg`: identical to curated tables. `dim_driver_scd2_changes_stg`: dim_driver_scd2 columns plus `change_action` (`CLOSE`, `OPEN`).

## MERGE statements (sql/merge/, parameters `@run_id`, `@run_date`)
- fact_rides: `MERGE ... ON T.ride_id = S.ride_id AND T.ride_date = S.ride_date WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`. Include a partition filter on `ride_date` range from the staging min/max.
- dim_driver_scd2: `ON T.driver_sk = S.driver_sk WHEN MATCHED AND S.change_action='CLOSE' THEN UPDATE SET effective_to, is_current=FALSE WHEN NOT MATCHED AND S.change_action='OPEN' THEN INSERT`.
- dim_zone: upsert by zone_id. rejected_records: delete-then-insert for `run_date`, in one script transaction.

## rh_ml
- Model `demand_arima_model`: `CREATE OR REPLACE MODEL` with `model_type='ARIMA_PLUS'`, `time_series_timestamp_col='demand_date'`, `time_series_data_col='ride_requests'`, `time_series_id_col='pickup_zone_id'`, `data_frequency='DAILY'`, `auto_arima=TRUE`, `holiday_region='IN'`. Training view: `rh_marts.v_zone_daily_demand` filtered to zones with at least `FORECAST_MIN_HISTORY_DAYS` days and dates before `run_date`.
- Table `demand_forecast_7d`: pickup_zone_id, forecast_date DATE, forecast_requests FLOAT64, lower_bound, upper_bound, confidence_level, model_trained_ts, run_id. Built from `ML.FORECAST(MODEL ..., STRUCT(7 AS horizon, 0.9 AS confidence_level))`. Replaced per `run_id` (delete-then-insert by run_date).

## rh_marts (views over curated and ml only)
`v_daily_ops_metrics` (ride_date, requests, completed, cancelled, cancellation_rate, gross_fare, avg_fare, avg_wait_seconds, avg_trip_seconds, active_drivers, data_through_ts), `v_zone_daily_demand` (pickup_zone_id, demand_date, ride_requests), `v_zone_hourly_demand`, `v_driver_performance` (rides and fare per driver joined to the SCD2 version valid at `requested_ts`), `v_demand_forecast_7d` (latest run only, with zone_name), `v_dq_summary`, `v_pipeline_freshness`. Views must expose only rows from runs whose ERROR DQ checks passed (join to `rh_ops.dq_results`, latest passed `run_id`).

## rh_ops
- `dq_results(run_id, run_date, check_id, severity, status, observed_value, threshold, detail, checked_ts)`; `pipeline_runs(run_id, run_date, status, started_ts, finished_ts)`; `cost_daily` snapshot from `INFORMATION_SCHEMA.JOBS` by label/user (sql/cost/).

## DQ checks (sql/dq/, each returns `check_id, severity, status, observed_value, threshold`)
| ID | Check | Severity |
|---|---|---|
| Q-001 | raw freshness: max ingest_ts within 6 h of run | ERROR |
| Q-002 | daily row count within 50% of trailing 7-day average | WARN |
| Q-003 | fact_rides ride_id unique | ERROR |
| Q-004 | exactly one current SCD2 row per driver | ERROR |
| Q-005 | no overlapping SCD2 intervals | ERROR |
| Q-006 | reject rate <= `DQ_MAX_REJECT_RATE` | ERROR |
| Q-007 | required columns non-null in fact_rides | ERROR |
| Q-008 | all fact_rides zones exist in dim_zone | ERROR |
| Q-009 | reconciliation: distinct raw rides in window = curated + rejected (tolerance 0) | ERROR |
| Q-010 | forecast has 7 rows for each eligible zone | WARN |

## Cost SQL (sql/cost/)
Bytes billed and estimated cost by day, dataset and job type for the last 30 days from `region-{BQ_LOCATION}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`. On-demand price is a parameter `@price_per_tib` (not hardcoded).
