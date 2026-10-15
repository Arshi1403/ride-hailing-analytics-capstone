# SQL: BigQuery warehouse and forecasting

Status: all SQL is written and checked **offline only** (file structure, parameters, column lists against the Spark and ingestion schemas, approximate syntax parse). **None of it has been run against BigQuery**, so BigQuery acceptance, MERGE behaviour and BQML training are not verified. No query, training or deployment runs without your approval.

## Layers and datasets
Datasets are fixed names; the project comes from the `bq --project_id` flag (default `labs-507309`), the location is `asia-south1` (edit the literal in `sql/ddl/00_datasets.sql` and the `region-asia-south1` qualifier in `sql/cost/` if it differs).

| Dataset | Holds | Written by |
|---|---|---|
| `raw_ds` | `rides_stream`, `rides_history` (+ Dataflow's `rides_stream_error_records`) | ingestion code (Dataflow, historical loader) |
| `stg_ds` | one run's Parquet output: `fact_rides_stg`, `rejected_rides_stg`, `dim_driver_scd2_stg` | Composer load jobs (WRITE_TRUNCATE) |
| `curated_ds` | `fact_rides`, `dim_driver_scd2`, `rejected_rides` | `sql/merge/*.sql` only |
| `ml_ds` | `zone_hourly_training`, `zone_forecast_eligibility`, `zone_demand_forecast`, model `zone_demand_arima` | `sql/bqml/*.sql` |
| `mart_ds` | views for Looker Studio | `sql/marts/*.sql` |
| `ops_ds` | `dq_results` | append the output of `sql/data_quality/*.sql` |

## What each object answers
| Object | Question |
|---|---|
| `curated_ds.fact_rides` | One clean row per ride: where, when, fare, trip minutes, surge, and which zone and driver data matched. Partitioned by `pickup_date`, clustered by `pickup_zone`, `driver_id`. |
| `curated_ds.dim_driver_scd2` | What were a driver's vehicle, home zone and status at any time? `effective_start_ts` / `effective_end_ts`, `is_current`. |
| `curated_ds.rejected_rides` | Which rides were rejected and why? |
| `mart_ds.v_daily_revenue` | Rides, revenue, average fare, surge share per day and source. |
| `mart_ds.v_zone_demand_hourly` | Rides per pickup zone per hour (actuals). |
| `mart_ds.v_driver_leaderboard` | Top drivers by revenue over the latest 30 days, with current attributes. |
| `mart_ds.v_zone_demand_forecast` | Predicted rides per top zone for the next 168 hours (with `forecast_ts`, `pickup_zone`, bounds, actual once known). |
| `ops_ds.dq_results` | Which checks passed, warned or failed for a run. |

## Interfaces with the other agents
- **Spark -> BigQuery:** column names and types of `fact_rides`, `rejected_rides` and the staging tables equal `spark/ridespark/schemas.py` (a test enforces it). Parquet folders: `rides/pickup_date=…`, `rejected/event_date=…`, `dim_driver_scd2/snapshot_date=…`. Load **only the latest `snapshot_date` partition** of the driver history into `stg_ds.dim_driver_scd2_stg`.
- **SCD2 dates to timestamps:** Spark gives `effective_from` / `effective_to` as dates in its `--timezone`. The merge converts them with `TIMESTAMP(date, @business_tz)`; pass the same timezone Spark used (`pickup_date` uses it too). The current version ends at `9999-12-31 00:00:00 UTC`.
- **Ingestion -> BigQuery:** raw table DDL equals `ingestion/schemas.py` (a test enforces it). `raw_ds.rides_stream_error_records` is created by the Dataflow template, not by this SQL.
- **Composer (contract for the DAG agent):** run order and parameters below. A run is identified by `run_id`; the window is the pickup-date window Spark recomputed (`pickup-start-date` .. `pickup-end-date`).
- **Orchestrator:** contract 03 (`rh_*` datasets, `cur`/`stg` names) is out of date. This SQL uses `raw_ds`, `stg_ds`, `curated_ds`, `mart_ds`, `ml_ds`, `ops_ds`.

## Run order and parameters
Windows PowerShell has no `<` redirection, so pipe the file. Add `--maximum_bytes_billed` to every query (value in bytes) and use `--dry_run` first.
```powershell
$bq = @("--project_id=labs-507309", "--location=asia-south1", "query", "--use_legacy_sql=false", "--maximum_bytes_billed=2000000000")
Get-Content sql\ddl\20_curated_fact_rides.sql -Raw | bq @bq                      # DDL, one-time
Get-Content sql\merge\merge_fact_rides.sql -Raw | bq @bq --parameter=run_id:STRING:run-001 `
  --parameter=window_start:DATE:2026-10-12 --parameter=window_end:DATE:2026-10-15
```
| Step | Files | Parameters |
|---|---|---|
| 0 once | `ddl/00..40` in order | none |
| 1 load | Composer loads Parquet into `stg_ds.*` | n/a |
| 2 merge | `merge/merge_dim_driver_scd2.sql` | `run_id`, `snapshot_date` (DATE), `business_tz` |
| | `merge/merge_fact_rides.sql` | `run_id`, `window_start`, `window_end` |
| | `merge/merge_rejected_rides.sql` | `run_id`, `window_start`, `window_end` |
| 3 quality gate | `data_quality/dq_*.sql` (6 files) | `run_id`, `window_start`, `window_end` (`business_tz` for `dq_row_counts`) |
| 4 marts | `marts/*.sql` (views, once; no refresh needed) | none |
| 5 forecast (approval) | `bqml/01..03`, then `04` | `as_of_ts` (TIMESTAMP, hour-aligned UTC), `train_days`, `top_n`, `min_rides`, `min_active_hours`, `run_id`, `confidence_level`; `04` takes `as_of_ts`, `top_n`, `run_id` |
| 6 cost | `cost/*.sql` | `days`, `top_n`, `price_per_tib`, `budget_usd` |
Append DQ output with `--destination_table ops_ds.dq_results --append_table` (the select list equals the table columns). **Any `status = FAIL` must stop steps 4 and 5.** `WARN` is reported only.

Suggested forecast values: `train_days` 56, `top_n` 10, `min_rides` 500, `min_active_hours` 168, `confidence_level` 0.9. `as_of_ts` is the end of the history you forecast from. The historical subset is NYC 2018 and the stream is current, so choose it at the end of the data you actually have (for example the end of the last loaded 2018 month); otherwise the training window is empty and step 1 stops with a clear message.

## Idempotency
- **Fact:** `MERGE` keyed on `(ride_id, pickup_date)` limited to the window. A rerun with the same `run_id` and content changes nothing. A new run updates rows, and rides that no longer exist in the recomputed window are deleted, because Spark recomputes whole pickup dates.
- **SCD2:** keyed on `(driver_id, effective_start_ts)`. Closed versions are never rewritten. A changed attribute arrives as a closed old version (updated) and a new current version (inserted). The only delete removes versions that start on or after the snapshot date and are no longer in the source (a corrected rerun of the latest snapshot). Older versions are never deleted.
- **Rejected rides:** delete the window and insert, in one transaction.
- **Forecast:** delete-and-insert per `as_of_ts` (older forecasts stay for back-testing). `CREATE OR REPLACE` is used only for views, `ml_ds.zone_hourly_training` and `ml_ds.zone_demand_arima`, which this pipeline owns.
- Every merge starts with `ASSERT` statements that stop it before any change when staging is empty, from another run, outside the window, duplicated, or when a driver does not have exactly one current version.

## Forecast design (top 10 zones, next 168 hours)
- One `ARIMA_PLUS` model, `time_series_id_col = pickup_zone`, `data_frequency = HOURLY`, `horizon = 168`. Output columns include `as_of_ts`, `forecast_ts`, `horizon_hour`, `pickup_zone`, name, borough, `predicted_rides`, bounds, `confidence_level`.
- **Top zones:** ranked by rides in the training window (ties by zone id), at most `top_n` (max 10). The series is read from `mart_ds.v_zone_demand_hourly`, as the capstone requires (create the views first).
- **Low-volume zones:** a zone must have at least `min_rides` rides and `min_active_hours` active hours in the window. Others are not trained; the reason (`LOW_VOLUME`, `TOO_FEW_ACTIVE_HOURS`) is stored in `ml_ds.zone_forecast_eligibility`. If fewer than 10 zones qualify, fewer are forecast.
- **Missing hours:** hours where no zone had any ride (outage, no data) are treated as gaps and left out, and ARIMA_PLUS interpolates them. Hours with rides citywide but none in a selected zone are real zero demand and are filled with 0. If an outage actually occurred during a busy hour in only one zone this still reads as 0; `FC-07`/`FC-08` in `04_validate_forecast.sql` report coverage.
- Predictions and bounds are floored at 0. `holiday_region` is not set because hourly holiday handling was not verified. Billing for ARIMA_PLUS training was not verified either.

## Cost safeguards
- Merge, DQ and forecast-validation queries filter on `pickup_date` (partition) and rely on clustering; the forecast series step reads the hourly mart view, which BigQuery cannot partition-prune, so it scans the narrow view columns of the whole fact table (bounded by `--maximum_bytes_billed`). Nothing scans `raw_ds` except two bounded checks (ingest dates window_start..window_end+7).
- Every file's header has a `Cost:` line and states that approval is needed. Use `--dry_run` and `--maximum_bytes_billed`. `fact_rides` does not force a partition filter, because the marts aggregate all dates; keep dashboard queries filtered by date and use caching.
- `sql/cost/cost_budget_check.sql` returns PASS/WARN/FAIL against a monthly budget so Composer can stop billable steps. The on-demand price and budget are parameters, never literals. The cost estimate is `bytes billed / 2^40 * price` and ignores credits, reservations and non-byte BQML pricing.
- `JOBS_BY_PROJECT` needs `bigquery.jobs.listAll` (for example `roles/bigquery.resourceViewer`).

## Validation
Offline (done): `pip install sqlglot pytest pyspark`, then `python -m pytest tests/sql -q` (the syntax and Spark-schema tests skip if `sqlglot` or `pyspark` is missing). It checks the expected files, headers (purpose, params, cost and approval), declared versus used `@parameters`, no hardcoded project, only the six known datasets, no `DROP`/`TRUNCATE`/unbounded `DELETE`, column lists against the Spark and ingestion schemas, partition and cluster keys, merge structure, forecast settings, DQ output columns, cost filters, and an approximate syntax parse with `sqlglot`. The parse cannot validate BigQuery scripting (`ASSERT`), BQML functions, or semantics.
With your approval and `gcloud auth login`: create the objects in a scratch dataset or the real one, then `--dry_run` each file; dry runs need the referenced tables to exist, so run them after the DDL. Report the bytes processed, or write "not validated".
Still unverified: that BigQuery accepts every statement (notably `ASSERT` with `NOT EXISTS`, `MERGE ... NOT MATCHED BY SOURCE` pruning with parameters, temp tables, `INTERVAL @param * 24 HOUR`, ML.FORECAST inside a transaction); ARIMA_PLUS in `asia-south1`; training cost and duration; the 30-day leaderboard's dynamic partition pruning.

## Dependencies
1. Spark job output (schemas, `run_id`, `--timezone`) and its Parquet layout; the zone file and driver snapshot sources are still unassigned (see `spark/README.md`).
2. Ingestion raw tables and the Dataflow error table.
3. Composer: loads Parquet into staging, passes parameters, runs the gate.
4. Infrastructure: datasets, IAM (`bigquery.dataEditor` on the datasets, `bigquery.jobUser`, `roles/bigquery.resourceViewer` for cost queries, Looker Studio read access to `mart_ds` only; consider authorized views so viewers do not need `curated_ds` access).
5. Orchestrator decisions: timezone (IST vs UTC), BQ location, and whether a refresh of the forecast is daily or weekly.
