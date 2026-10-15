# Contract 00: Conventions and Configuration (v0.1.0, draft)

Affected agents: all.

## Identity defaults (config defaults only, not proof that resources exist)
| Key | Default |
|---|---|
| `GCP_PROJECT_ID` | `labs-507309` |
| `GCP_REGION` | `asia-south1` |
| `BQ_LOCATION` | `asia-south1` |
| `BUSINESS_TZ` | `Asia/Kolkata` |
| `RESOURCE_PREFIX` | `rh` |

## Naming
- snake_case for columns and tables. Timestamps end `_ts` (UTC `TIMESTAMP`). Dates end `_date` (`DATE`, business-day in `BUSINESS_TZ`).
- Resources use `rh-` prefix with hyphens; BigQuery datasets use `rh_` with underscores.

## Resources (names are requests to infrastructure-security-agent; discover before creating)
| Type | Config key | Name |
|---|---|---|
| Bucket | `GCS_LANDING_BUCKET` | `labs-507309-rh-landing` |
| Bucket | `GCS_CURATED_BUCKET` | `labs-507309-rh-curated` |
| Bucket | `GCS_ARTIFACTS_BUCKET` | `labs-507309-rh-artifacts` (Dataflow temp, Spark code, Dataproc staging) |
| Pub/Sub topic | `PUBSUB_TOPIC` | `rh-ride-events` |
| Subscription | `PUBSUB_SUBSCRIPTION` | `rh-ride-events-dataflow` |
| Dead-letter topic | `PUBSUB_DLQ_TOPIC` | `rh-ride-events-dlq` |
| Dataset | `BQ_RAW_DATASET` | `rh_raw` |
| Dataset | `BQ_STAGING_DATASET` | `rh_staging` |
| Dataset | `BQ_CURATED_DATASET` | `rh_curated` |
| Dataset | `BQ_MARTS_DATASET` | `rh_marts` |
| Dataset | `BQ_ML_DATASET` | `rh_ml` |
| Dataset | `BQ_OPS_DATASET` | `rh_ops` |
| Service account | `SA_PRODUCER` | `sa-rh-producer` |
| Service account | `SA_DATAFLOW` | `sa-rh-dataflow` |
| Service account | `SA_SPARK` | `sa-rh-spark` |
| Service account | `SA_COMPOSER` | `sa-rh-composer` |

## GCS layout
```
gs://{LANDING}/historical/trips/batch_date=YYYY-MM-DD/*.csv
gs://{LANDING}/historical/drivers/batch_date=YYYY-MM-DD/*.csv
gs://{LANDING}/reference/zones.csv
gs://{LANDING}/_loaded/{batch_date}/...      load markers
gs://{CURATED}/fact_rides/ride_date=YYYY-MM-DD/*.parquet
gs://{CURATED}/dim_driver_scd2_changes/run_date=YYYY-MM-DD/*.parquet
gs://{CURATED}/dim_driver_scd2_snapshot/run_date=YYYY-MM-DD/*.parquet
gs://{CURATED}/dim_zone/*.parquet
gs://{CURATED}/rejected_records/run_date=YYYY-MM-DD/*.parquet
gs://{CURATED}/_manifests/run_date=YYYY-MM-DD/{run_id}.json
gs://{ARTIFACTS}/dataflow/{temp,staging,templates}/
gs://{ARTIFACTS}/spark/{code,staging}/
```

## Other config keys
`RUN_DATE` (YYYY-MM-DD), `RUN_ID` (`{run_date}T{HHMMSS}Z-{short uuid}`), `SPARK_RUNTIME_VERSION` (`2.2`), `DATAPROC_SUBNET`, `DATAFLOW_MAX_WORKERS` (default 2), `PRODUCER_DRY_RUN` (default `true`), `PRODUCER_MAX_EVENTS` (default 100), `PRODUCER_SEED`, `COMPOSER_ENV_NAME`, `FORECAST_HORIZON_DAYS` (7), `FORECAST_MIN_HISTORY_DAYS` (30), `DQ_MAX_REJECT_RATE` (0.05), `LOOKBACK_DAYS` (3).
All keys appear in `.env.example` (placeholders only) and Airflow Variables use the lowercase form prefixed `rh_`.

## Python package layout
`src/ingestion/{producer,dataflow,batch}`, `src/spark/{jobs,transforms,scd2,io}`, `src/warehouse/`. Python 3.11. Tests with pytest.
