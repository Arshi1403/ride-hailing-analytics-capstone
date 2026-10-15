# GCP resource map: what gets created, where it lives, how to show it

Nothing below exists until you run `bootstrap.sh --apply` and the pipeline (approved by you). Names are the defaults from the code; the three bucket names come from `RH_LANDING_BUCKET`, `RH_CURATED_BUCKET`, `RH_ARTIFACTS_BUCKET` (defaults used by `bootstrap.sh`: `<project>-rh-landing`, `-rh-curated`, `-rh-artifacts`). Project `labs-507309`, location `asia-south1`.

## Cloud Storage
| Bucket | Path | Written by | Contents |
|---|---|---|---|
| `labs-507309-rh-landing` | `historical/<date>/yellow_tripdata_2018_*.csv` | you (upload) | NYC Taxi 2018 subset; the DAG sensor waits for it |
| | `reference/taxi_zone_lookup.csv` | you | zone names for enrichment |
| | `drivers/snapshot.*` (optional) | you / a future producer | daily driver snapshot for SCD2 |
| | `raw/` | not used by the BigQuery path | lifecycle rule deletes it after 14 days |
| `labs-507309-rh-curated` | `curated/rides/pickup_date=YYYY-MM-DD/*.parquet` | Spark | curated rides, partitioned Parquet |
| | `curated/rejected/event_date=.../` | Spark | rejected rides with reason |
| | `curated/dim_driver_scd2/snapshot_date=.../` | Spark | full driver history per snapshot |
| | `curated/driver_snapshot_conflicts/`, `curated/_run_metrics/processing_date=.../run_id=.../part-*.txt` | Spark | conflicts, run counts as JSON (nice for the demo) |
| `labs-507309-rh-artifacts` | `spark/code/run_job.py`, `spark/code/ridespark.zip` | `bootstrap.sh --upload-code` | Spark code |
| | `dataflow/udf/ride_event_udf.js`, `dataflow/temp/` | upload / Dataflow | UDF and temp files (14-day lifecycle) |
| Composer's own bucket (auto-created) | `dags/`, `data/sql/` | you | the DAG, helpers and the SQL files |

## Pub/Sub and Dataflow
- Topic `rides-events` (producer publishes), subscription `rides-events-sub` (Dataflow reads).
- Dataflow job `rides-pubsub-to-bq` (Console > Dataflow > Jobs): streaming, max 2 workers. Cancel it after the test.

## BigQuery (all in `asia-south1`)
| Dataset | Table / view | Filled by | Demo query |
|---|---|---|---|
| `raw_ds` | `rides_stream` | Dataflow | `SELECT COUNT(*), MIN(ingest_ts), MAX(ingest_ts) FROM raw_ds.rides_stream` (expect 1,000 after the full run) |
| | `rides_stream_error_records` | Dataflow template | invalid events with the error message |
| | `rides_history` | Composer (GCS load + merge) | `SELECT source_file, COUNT(*) FROM raw_ds.rides_history GROUP BY 1` |
| | `rides_history_stg` | Composer | staging of the last CSV (replaced per run) |
| `stg_ds` | `fact_rides_stg`, `rejected_rides_stg`, `dim_driver_scd2_stg` | Composer (Parquet load) | one run's Spark output |
| `curated_ds` | `fact_rides` | MERGE | partitioned by `pickup_date`, clustered by `pickup_zone, driver_id` (Table details tab) |
| | `dim_driver_scd2` | MERGE | `SELECT * FROM curated_ds.dim_driver_scd2 WHERE driver_id = 'D00042' ORDER BY effective_start_ts` (shows history) |
| | `rejected_rides` | MERGE | `SELECT reject_reason, COUNT(*) FROM curated_ds.rejected_rides GROUP BY 1` |
| `mart_ds` | `v_daily_revenue`, `v_zone_demand_hourly`, `v_driver_leaderboard`, `v_zone_demand_forecast` | views | the dashboard sources |
| `ml_ds` | model `zone_demand_arima`, `zone_hourly_training`, `zone_forecast_eligibility`, `zone_demand_forecast` | weekly branch | `SELECT * FROM mart_ds.v_zone_demand_forecast ORDER BY pickup_zone, forecast_ts LIMIT 200` |
| `ops_ds` | `dq_results` | `dq_checks` task | `SELECT check_id, status, observed_value FROM ops_ds.dq_results ORDER BY checked_ts DESC LIMIT 50` |

## Dataproc Serverless and Composer
- Batches: Console > Dataproc > Batches, ids `rides-curation-<timestamp>-<try>`. Logs show the Spark metrics line (rows in, duplicates removed, rejected by reason).
- Composer: Airflow UI > DAG `ride_hailing_daily` (graph view shows the ten tasks; XCom tab of `dq_checks` shows the compact summary).
- Secret Manager: `airflow-connections-rh_alert_webhook` (optional alert webhook).

## Reading order for the demo
1. Bucket `landing` -> topic and Dataflow job -> `raw_ds.rides_stream` counts.
2. `rides_history` load via Composer.
3. Dataproc batch, then the curated bucket folders and `_run_metrics`.
4. `stg_ds` -> `curated_ds` MERGE results; fact table details (partition and cluster); SCD2 history for one driver.
5. DQ results, marts, forecast table, Looker Studio.
6. Cost report (daily bytes billed, expensive queries).
