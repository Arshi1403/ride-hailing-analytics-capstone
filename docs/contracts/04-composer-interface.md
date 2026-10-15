# Contract 04: Composer DAG Interface (v0.1.0, draft)

Affected agents: composer, spark, bigquery, ingestion, infrastructure, testing.

## DAGs
| DAG id | Schedule | Purpose |
|---|---|---|
| `rh_daily_pipeline` | `0 1 * * *`, tz `Asia/Kolkata`, `catchup=False`, `max_active_runs=1` | Daily workflow for the previous business day |
| `rh_historical_load` | manual trigger, params `batch_date` | Load historical CSVs and zones to raw |

Streaming (producer, Dataflow) runs outside Composer. The daily DAG only checks freshness.

## Run parameters
`run_date = {{ data_interval_start | ds }}` (previous business day). `run_id` = `{{ run_date }}T{{ ts_nodash }}Z-{{ ti.try_number }}`. Airflow Variables: `rh_` + lowercase config key from contract 00.

## `rh_daily_pipeline` tasks (in order)
| Task id | Action | Input contract | Retries |
|---|---|---|---|
| `check_raw_freshness` | SQL check Q-001 | 03 | 2 |
| `load_historical_if_new` | trigger/short-circuit on new `historical/` batches | 01 | 1 |
| `spark_curation` | `DataprocCreateBatchOperator` with `run_curation.py` args from contract 02 | 02 | 1 |
| `load_staging` | GCS Parquet -> `rh_staging.*` (WRITE_TRUNCATE) | 02, 03 | 2 |
| `merge_curated` | run `sql/merge/*.sql` with `@run_id`, `@run_date` | 03 | 1 |
| `run_dq_checks` | run `sql/dq/*.sql`, insert to `rh_ops.dq_results` | 03 | 0 |
| `dq_gate` | fail the DAG if any ERROR check failed; marts and forecast do not run | 03 | 0 |
| `train_forecast_model` | `sql/ml/train_arima.sql` | 03 | 0 |
| `generate_forecast` | `sql/ml/forecast_7d.sql` | 03 | 1 |
| `record_cost_snapshot` | `sql/cost/*.sql` | 03 | 1 |
| `mark_run_success` | write `rh_ops.pipeline_runs` | 03 | 1 |

Failure callbacks write `pipeline_runs.status='FAILED'`. SLA: `sla` of 04:30 hours after midnight IST on `mark_run_success` with a miss callback at 05:30 IST. `retry_delay` 5 min, `execution_timeout` per task documented in code.

## Rules
- DAG files import with no network or credentials. Resource names via Variables with defaults from contract 00.
- Tasks are idempotent by `run_id`/`run_date`.
- Composer calls SQL files by path from `sql/` and Spark by the contract 02 args. It never embeds schema or SQL text.
- Service account `sa-rh-composer`. Billable runs (Dataproc, BigQuery ML) are triggered only after user approval; the DAG ships paused (`is_paused_upon_creation=True`).
