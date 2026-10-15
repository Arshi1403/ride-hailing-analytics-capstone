# Contract 02: Spark Job Interface (v0.1.0, draft)

Affected agents: spark, bigquery, composer, testing.

## Entry point
`src/spark/jobs/run_curation.py` submitted as a Dataproc Serverless batch.

| Arg | Meaning |
|---|---|
| `--project-id` | `GCP_PROJECT_ID` |
| `--run-date` | business date to process, YYYY-MM-DD |
| `--run-id` | `RUN_ID` |
| `--raw-dataset`, `--curated-dataset` | dataset names |
| `--curated-uri` | `gs://{CURATED}` |
| `--lookback-days` | re-read raw for late data, default 3 |
| `--mode` | `incremental` or `backfill` |
| `--local-input` / `--local-output` | local Parquet/CSV paths for tests, mutually exclusive with BigQuery/GCS |

Exit code 0 only if outputs and manifest were written. The manifest `_manifests/run_date=.../{run_id}.json` has `run_id`, `run_date`, row counts per output, `rejected_count`, `status`.

## Inputs
`rh_raw.ride_events_raw`, `rh_raw.driver_events_raw`, `rh_raw.hist_trips_raw`, `rh_raw.hist_drivers_raw`, `rh_raw.zones_raw`, and current rows of `rh_curated.dim_driver_scd2` (via BigQuery connector; empty on first run). Schemas are in contract 03.

## Processing rules
1. Dedupe events on `event_id`; dedupe historical on `ride_id`.
2. Build one row per ride from events (state: requested -> assigned -> started -> completed or cancelled). If a ride exists in both sources, keep the one with the later `last_event_ts`; `source` = `stream`, `historical` or `both`.
3. Business date `ride_date = DATE(requested_ts, BUSINESS_TZ)`. Output only rides with `ride_date` in [run_date - lookback_days, run_date].
4. Apply row rules, rejected rows go to `rejected_records` with rule id:
 - R-001 ride_id non-null; R-002 timestamp order requested <= started <= completed; R-003 0 <= fare <= 10000; R-004 0 < distance <= 500; R-005 zones exist in dim_zone; R-006 currency INR; R-007 surge 1.0..5.0; R-008 COMPLETED needs driver_id and fare; R-009 no timestamps later than now + 5 min; R-010 CANCELLED needs cancellation_reason.
 - D-001 driver_id non-null; D-002 status in enum; D-003 home_zone_id exists; D-004 rating 0..5.
5. SCD2: take driver changes ordered by `event_ts`; compute `record_hash` over tracked attributes (`city, vehicle_type, status, home_zone_id, rating_band`); `rating_band` = LOW (<3.5), MID (3.5 to 4.5), HIGH (>4.5). A new version is created only when the hash differs from the previous version. Output `CLOSE` rows (existing current row with `effective_to` = new `effective_from`, `is_current` false) and `OPEN` rows (new version, `effective_to` = `9999-12-31 23:59:59 UTC`, `is_current` true). Out-of-order events older than the current version's `effective_from` are rejected with D-005.
6. Reruns for the same `run_date` must give the same output (dynamic partition overwrite, deterministic `driver_sk`).

## Outputs (Parquet, snappy; schemas in contract 03 under `rh_curated` and `rh_staging`)
`fact_rides/ride_date=`, `dim_driver_scd2_changes/run_date=`, `dim_driver_scd2_snapshot/run_date=`, `dim_zone/`, `rejected_records/run_date=`.
Parquet column names and types must equal the staging table schemas exactly (TIMESTAMP as `timestamp[us]`, NUMERIC as decimal(38,9)).
