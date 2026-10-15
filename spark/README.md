# Spark processing layer

PySpark job that turns raw streaming and historical ride data into curated, partitioned Parquet. Package: `spark/ridespark/`. Tests: `tests/spark/`.

Status: 109 Spark tests pass locally on Windows (Python 3.11, Java 17, PySpark 3.5.3) and 5 are skipped; see "Tests" below. **Nothing has been submitted to Dataproc or read from BigQuery.** The BigQuery connector path (`bq://` sources) and the Dataproc command are unverified.

## Data flow
```
raw_ds.rides_stream (stream)  ┐                          ┌ <output>/rides/pickup_date=YYYY-MM-DD/      curated rides
raw_ds.rides_history    ├─ normalize ─ window ─ dedupe ─ validate ─ derive ─ enrich ┤ <output>/rejected/event_date=YYYY-MM-DD/    quarantine
zone dimension (CSV/Parquet)  ┤                                                            ├ <output>/dim_driver_scd2/snapshot_date=…/   driver history
driver snapshot (daily)       ┘                                                            └ <output>/_run_metrics/…                     counts as JSON
```

## Rules
| Step | Rule |
|---|---|
| Schemas | Explicit, no inference. Missing columns or incompatible types stop the job (exit 4) with the column names. Integer widening and numeric-to-decimal/double are allowed; text-to-timestamp is not. Extra columns are ignored. |
| Unified schema | Streaming and historical rows become the same 13 columns. A field a source does not have stays null: historical has no driver, rider or surge; streaming has no distance. |
| Historical ride_id | TLC data has no trip id, so `ride_id = "H-" + sha256(VendorID, pickup, dropoff, PU zone, DO zone, fare)`. Trips identical in all of those fields collapse into one. |
| Distance | `distance_km = trip_distance (miles) * 1.609344` when `trip_distance > 0`, else null. |
| Window | A run recomputes pickup dates from `--pickup-start-date` to `--pickup-end-date` (default: processing date minus `--lookback-days`, 3, up to processing date). Rows without a pickup time are placed by ingest date. |
| Dedupe | Per `ride_id`: latest `ingest_ts`; ties: stream before historical, then larger `source_file`, then larger content hash. Null `ingest_ts` loses. Independent of input order. Dedupe happens **before** validation, so if the winning copy is invalid it is rejected and an older valid copy is not resurrected. Duplicates are counted (`duplicates_removed`), not quarantined. |
| Reject (first match) | `NULL_RIDE_ID`, `NULL_PICKUP_TS`, `NULL_DROPOFF_TS`, `NULL_FARE`, `NEGATIVE_FARE`, `NEGATIVE_TRIP_DURATION`, `TRIP_TOO_LONG` (> 4 h; exactly 4 h is valid). `NULL_FARE` and `NEGATIVE_TRIP_DURATION` go beyond the stated rules because revenue and trip_minutes need them; zero fares are valid. |
| Derived | `trip_minutes`, `revenue_per_km` (needs `distance_km > 0`), `surge_flag` (`surge_multiplier > 1.0`, null when surge unknown), `weekday` (`Mon`..`Sun`), `hour_bucket` by local pickup hour: NIGHT 0-5, MORNING_PEAK 6-9, MIDDAY 10-15, EVENING_PEAK 16-19, LATE_EVENING 20-23. |
| Time zone | `--timezone` (default UTC) decides `pickup_date`, `weekday`, `hour_bucket`. NYC historical timestamps are stored as given (treated as UTC). The orchestrator's assumption D-006 says IST; confirm before production. |

### Processing date vs pickup date
- `pickup_date`: the trip's date in `--timezone`. It is the **partition key**. A row always lives under its pickup date, and a partition's contents depend only on raw rows of that date.
- `processing_date`: the date the run is attributed to (`--processing-date`, e.g. Composer's `ds`). It is an ordinary **audit column**, never a partition. Reprocessing later overwrites it with the later run's value, as `run_id` is.
- Rejected rows are partitioned by `event_date` (pickup date, else ingest date, else processing date).

### Safe reruns
Outputs use dynamic partition overwrite: a run replaces only the partitions of its window, and the same input gives the same output (tests: identical content after repeated runs, other partitions untouched). Limits: a ride whose copies carry different pickup dates is deduplicated only inside one window; a partition that should become empty is not removed.

## SCD Type 2 drivers
Input: a **daily snapshot**, one row per driver: `driver_id, vehicle_type, home_zone, status` (tracked attributes; `DRIVER_TRACKED_COLUMNS` in `schemas.py`). No other attributes are used or invented. History columns: `driver_id, vehicle_type, home_zone, status, record_hash, effective_from, effective_to, is_current` (dates; valid for `effective_from <= d < effective_to`; current ends `9999-12-31`).
- New driver: one current version starting on the snapshot date (the true start date is unknown).
- Changed tracked attribute: current version closed (`effective_to` = snapshot date, `is_current` false), new current version opened. Prior versions are kept.
- Unchanged: nothing happens. Null attributes compare null-safely.
- Driver absent from a snapshot: left unchanged (absence is not deletion; send `status`, e.g. `INACTIVE`).
- Conflicting rows (same `driver_id`, different attributes, or null `driver_id`): ignored, driver unchanged, rows written to `<output>/driver_snapshot_conflicts/`.
- Repeating the same snapshot date is a no-op; rerunning the latest date with corrected data replaces that day's changes. A date older than the latest stored version is refused (exit 5).
- Every run checks: exactly one current record per driver, no gaps or overlaps. Failure exits 5 before writing.
- Each run stores the full state in `dim_driver_scd2/snapshot_date=<date>/`; the latest partition is the current dimension.

## Dimensions and what happens when they are unavailable
| Situation | Result |
|---|---|
| Zone or driver dimension not configured, unreadable, or empty | **Job fails before writing anything** (exit 3, clear message). |
| Same, with `--allow-missing-dimensions` | Job continues. Dimension attributes are null and `zone_match` / `driver_match` = `DIMENSION_UNAVAILABLE`. |
| Dimension present but its schema is wrong, or zone ids are duplicated | Job fails (exit 4). Never skipped silently. |
| Ride's zone not in the dimension | Row kept, names null, `zone_match` = `PICKUP_NOT_FOUND`, `DROPOFF_NOT_FOUND` or `BOTH_NOT_FOUND`. |
| Ride without `driver_id` (all historical rides) | Row kept, driver attributes null, `driver_match` = `NO_DRIVER_ID`. |
| Driver unknown, or ride earlier than the driver's first version | Row kept, attributes null, `driver_match` = `DRIVER_NOT_FOUND`. History is not back-filled. |
Driver enrichment is point in time: the version valid on the ride's `pickup_date`.
Downstream users should filter on `*_match` when they need complete dimensions.

## Interfaces
### Inputs (owned by ingestion, orchestrator contracts 01 and 03 need updating)
| Argument | Source | Columns used |
|---|---|---|
| `--stream-input` | `bq://labs-507309.raw_ds.rides_stream` or Parquet | `ride_id, driver_id, rider_id, pickup_zone, dropoff_zone, pickup_ts, dropoff_ts, fare, surge_multiplier, ingest_ts` |
| `--hist-input` | `bq://labs-507309.raw_ds.rides_history` or Parquet | `VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, PULocationID, DOLocationID, fare_amount, source_file, ingest_ts` |
| `--zones-input` | TLC `taxi_zone_lookup.csv` (`LocationID, Borough, Zone, service_zone`), or Parquet/BigQuery with `zone_id, zone_name, borough, service_zone` | all |
| `--driver-snapshot` | CSV/Parquet/BigQuery | `driver_id, vehicle_type, home_zone, status` |
**Open request:** nothing produces the zone file or driver snapshot yet. The orchestrator must assign an owner and path, or tell us to leave drivers unavailable.

### Outputs (for the BigQuery agent) under `--output-path`
Column names and types are fixed in `ridespark/schemas.py`; `python -c "from ridespark.schemas import *; import json; print(json.dumps(bigquery_schema(CURATED_RIDES_SCHEMA)))"` (run from `spark/`) prints the BigQuery column list. A test asserts the written data equals these schemas.
| Folder | Partition | Type mapping |
|---|---|---|
| `rides/` | `pickup_date` (DATE) | decimal(38,9) -> NUMERIC, timestamp -> TIMESTAMP (micros), double -> FLOAT64, boolean -> BOOL |
| `rejected/` | `event_date` (DATE) | same |
| `dim_driver_scd2/` | `snapshot_date` (DATE) | full state per snapshot; load the latest partition |
| `driver_snapshot_conflicts/`, `_run_metrics/` | | diagnostics |
Curated ride columns: `ride_id, source, source_file, driver_id, rider_id, pickup_zone, dropoff_zone, pickup_ts, dropoff_ts, fare, surge_multiplier, distance_km, ingest_ts, trip_minutes, revenue_per_km, surge_flag, weekday, hour_bucket, pickup_zone_name, pickup_borough, dropoff_zone_name, dropoff_borough, zone_match, driver_vehicle_type, driver_home_zone, driver_status, driver_version_from, driver_match, processing_date, run_id, pickup_date`.
Loading into BigQuery: use hive partitioning on `pickup_date` and load/MERGE by `ride_id` plus `pickup_date`.

### For the orchestrator / Composer
Entry point `spark/run_job.py` (needs `ridespark.zip`). Arguments: `--processing-date` (required, YYYY-MM-DD, pass `{{ ds }}`), `--output-path` (required), `--stream-input`, `--hist-input` (at least one), `--zones-input`, `--driver-snapshot`, `--snapshot-date`, `--lookback-days`, `--pickup-start-date`, `--pickup-end-date`, `--timezone`, `--run-id`, `--allow-missing-dimensions`. Exit codes: 0 ok, 1 unexpected, 2 bad arguments, 3 dimension unavailable, 4 schema mismatch, 5 driver-history integrity.
The daily run uses the default window; historical backfill uses an explicit window of at most 366 days.

## Local run (no cloud)
```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r spark/requirements.txt            # needs Java 8/11/17
python -m pytest tests/spark -q
cd spark
python run_job.py --processing-date 2026-10-15 --stream-input C:\data\stream_parquet --hist-input C:\data\hist_parquet `
  --zones-input C:\data\taxi_zone_lookup.csv --driver-snapshot C:\data\drivers.csv --output-path C:\data\curated
```

## Dataproc Serverless submission (requires explicit approval; none has been run)
```powershell
python spark/build_package.py                                   # builds spark/dist/ridespark.zip
# set variables from spark/config.example.env, then print the command:
cd spark; python -m ridespark.submit --processing-date 2026-10-15
```
Upload `run_job.py` and `ridespark.zip` to `gs://<bucket>/spark/code/`, review the printed `gcloud dataproc batches submit pyspark ...` command, get approval, and run it. The service account needs: `roles/dataproc.worker` (project), `roles/bigquery.jobUser` and `roles/bigquery.readSessionUser` (project), `roles/bigquery.dataViewer` on `raw_ds`, `roles/storage.objectAdmin` on the output bucket and deps bucket, `roles/storage.objectViewer` on dimension files. The subnet needs Private Google Access.
Cloud validations still required: the command's flags and runtime `2.2` against current Dataproc docs; that the runtime includes the BigQuery connector and Python 3.11; a real read of `raw_ds.*` through `bq://`; a small-window run; output compared with BigQuery; cost check.

## Tests
`python -m pytest tests/spark -q` (about 2 minutes). Covered: valid and invalid rows (each reject reason and the 4-hour boundary), duplicates and tie-breaks, schema mismatches (missing columns, wrong types, streaming vs historical), enrichment and missing dimensions, derived columns, SCD2 (new, unchanged, changed, repeated snapshots, reruns, conflicts, integrity checks), job exit codes with in-memory I/O, output schema equal to the published contract, and the submit command.
Result on this machine: 109 passed, 5 skipped. The 5 skipped tests (`test_spark_job_parquet_io.py`: real Parquet partition layout, rerun-with-files, partition-only overwrite) need local Parquet I/O, which fails here with `UnsatisfiedLinkError ... NativeIO$Windows` because Hadoop's `winutils.exe`/`hadoop.dll` are not installed. They run unchanged on Linux/macOS/CI or Windows with `HADOOP_HOME` set. Until then, partition overwrite behaviour is **not verified**; the in-memory tests prove the transformation is deterministic per window, not the file layout.

## Troubleshooting
| Symptom | Fix |
|---|---|
| exit 3 | Provide `--zones-input` / `--driver-snapshot` (or the stored history), or add `--allow-missing-dimensions`. |
| exit 4, "found string, expected timestamp" | The source writes text timestamps; fix the source schema. |
| exit 5, "after snapshot date" | Only the latest snapshot date can be rerun. |
| Local tests fail to start Spark | Check `java -version`; set `JAVA_HOME`. On Windows, if Parquet writes fail with `HADOOP_HOME` / `winutils` errors, install winutils for Hadoop 3 and set `HADOOP_HOME`. |
| `pickup_date` looks one day off | Set `--timezone`; the default is UTC. |
