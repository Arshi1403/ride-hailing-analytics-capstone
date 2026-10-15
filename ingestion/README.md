# Ingestion: streaming and historical

Status: code and unit tests exist and pass locally. **Nothing here has been run against GCP.** See "Cloud validations still required".

```
producer/   mock event producer (Pub/Sub)         -> producer/README.md
ingestion/  schemas + DDL, Dataflow launch helper, JS UDF, historical CSV loader, verification SQL
```

## Event schema (streaming)
| Column | BigQuery type | Notes |
|---|---|---|
| ride_id | STRING | unique id; dedupe key (Pub/Sub delivery is at-least-once) |
| driver_id, rider_id | STRING | `[A-Za-z0-9_-]{1,64}` |
| pickup_zone, dropoff_zone | STRING | NYC TLC taxi zone id (1 to 265), same id space as `PULocationID` in the historical data |
| pickup_ts, dropoff_ts | TIMESTAMP | JSON text `YYYY-MM-DDTHH:MM:SSZ` (UTC); dropoff not before pickup, trip at most 6 h |
| fare | NUMERIC | JSON number, > 0 and <= 1000 (INR) |
| surge_multiplier | FLOAT64 | 1.0 to 5.0 |
| ingest_ts | TIMESTAMP | **added at the ingestion boundary**, see below |

Validation is applied twice with the same rules: the producer before publishing (`producer/schema.py`), and the Dataflow UDF at ingestion (`ingestion/udf/ride_event_udf.js`). A test keeps the constants in sync.

### `ingest_ts`
- Type: `TIMESTAMP`, UTC, `REQUIRED`.
- Source: the Dataflow UDF sets it to the worker clock (`new Date().toISOString()`) when the message is processed. The producer never sends it, and a producer payload containing it is invalid.
- Meaning: when the event entered the raw layer, so `ingest_ts - pickup_ts` is end-to-end lag. The raw table is partitioned by `DATE(ingest_ts)`.
- Historical table: `ingest_ts` is the load time (`CURRENT_TIMESTAMP()` inside the load transaction).

## Dataflow template: Pub/Sub **Subscription** to BigQuery
Template: Google-provided classic template `PubSub_Subscription_to_BigQuery` at `gs://dataflow-templates-<region>/latest/PubSub_Subscription_to_BigQuery`.

**Use the subscription, not the topic.** The topic variant creates a temporary subscription when the job starts, so events published while the job is stopped or restarting are lost. A durable subscription keeps them (retention 7 days in the commands below). You create `rides-events-sub` on topic `rides-events` once; the producer publishes to the topic only.

| Parameter | Value | Required |
|---|---|---|
| `inputSubscription` | `projects/labs-507309/subscriptions/rides-events-sub` | yes |
| `outputTableSpec` | `labs-507309:raw_ds.rides_stream` (`project:dataset.table`; table must already exist with the schema above) | yes |
| `outputDeadletterTable` | `labs-507309:raw_ds.rides_stream_error_records` (template creates it if absent) | no, default is `<outputTableSpec>_error_records` |
| `javascriptTextTransformGcsPath` | `gs://<bucket>/dataflow/udf/ride_event_udf.js` | needed here (adds `ingest_ts`, validates) |
| `javascriptTextTransformFunctionName` | `process` | needed with the path |
Launch settings: `--region`, `--gcs-location` (template), `--staging-location`, `--service-account-email`, `--max-workers` (default 2).

The destination is configurable with `BQ_RAW_DATASET`, `BQ_STREAM_TABLE` (and `BQ_DLQ_TABLE`). Rows that fail the UDF or the BigQuery insert go to the dead-letter table with an error message; nothing is dropped silently.

Print the launch command (it is never executed by the code):
```powershell
python -m ingestion.dataflow
```
The template name, parameter names and the regional template bucket are written from knowledge of Google's documentation and must be checked against the current docs and the real bucket before launch (see validations).

## Configuration
Environment variables, documented in `ingestion/config.example.env` and `producer/config.example.env`. Defaults: project `labs-507309`, region `asia-south1`, topic `rides-events`, dataset `raw_ds`, stream table `rides_stream`, historical table `rides_history`, subscription `rides-events-sub`. These are proposals, not evidence that resources exist. Invalid values stop the program with a readable message before any cloud call. Authentication is Application Default Credentials only (`gcloud auth application-default login`); no keys in the repo.

## Setup (local, safe)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r producer/requirements.txt -r ingestion/requirements.txt pytest
python -m pytest tests/producer tests/ingestion -q
```
Run from the repo root. Add `.venv/` to `.gitignore` (owned by infrastructure).

## Deployment (each step needs your approval; none has been run)
1. **Discover (read-only):**
```powershell
gcloud config set project labs-507309
gcloud pubsub topics list --format="value(name)"
gcloud pubsub subscriptions list --format="value(name)"
bq ls --project_id=labs-507309
bq ls labs-507309:raw_ds
gcloud storage buckets list --format="value(name,location)"
gcloud iam service-accounts list --format="value(email)"
gcloud dataflow jobs list --region=asia-south1 --status=active
gcloud storage ls gs://dataflow-templates-asia-south1/latest/PubSub_Subscription_to_BigQuery
```
2. **Create only what is missing** (review the DDL first; nothing is executed by the Python code):
```powershell
python -m ingestion.schemas all            # prints dataset, stream table and historical table DDL
gcloud pubsub topics create rides-events
gcloud pubsub subscriptions create rides-events-sub --topic=rides-events --ack-deadline=60 --message-retention-duration=7d --expiration-period=never
gcloud storage cp ingestion/udf/ride_event_udf.js gs://<bucket>/dataflow/udf/ride_event_udf.js
```
3. **IAM** (below), then **launch**: set `DATAFLOW_TEMP_LOCATION`, `DATAFLOW_UDF_GCS_PATH`, `DATAFLOW_SERVICE_ACCOUNT`, run `python -m ingestion.dataflow`, review, and run the printed command.
4. **Stop the job when done** to avoid cost: `gcloud dataflow jobs cancel <JOB_ID> --region=asia-south1` (streaming jobs run until stopped).

## IAM and service accounts (requests; nothing granted)
| Identity | Roles | Scope |
|---|---|---|
| Producer runner (your user or `sa-producer`) | `roles/pubsub.publisher` | topic `rides-events` |
| Dataflow worker service account (`DATAFLOW_SERVICE_ACCOUNT`) | `roles/dataflow.worker` | project |
| | `roles/pubsub.subscriber` | subscription `rides-events-sub` |
| | `roles/bigquery.dataEditor` | dataset `raw_ds` |
| | `roles/bigquery.jobUser` | project |
| | `roles/storage.objectAdmin` | the temp/staging bucket (also holds the UDF) |
| Person launching the job | `roles/dataflow.developer`; `roles/iam.serviceAccountUser` on the worker service account | project / that service account |
| Historical loader identity | `roles/bigquery.jobUser` (project), `roles/bigquery.dataEditor` on `raw_ds`, `roles/storage.objectViewer` on the CSV bucket | as listed |
No owner or editor roles; one worker service account per purpose.

## Historical load: NYC Taxi **2018** CSV to BigQuery
Dataset requirement: a subset of the **NYC TLC yellow taxi 2018** data. Enforced in three places so another year cannot slip in:
1. `HIST_YEAR` must be 2018 and the file name must match `HIST_FILE_PATTERN` (default: contains `2018`, ends `.csv` or `.csv.gz`).
2. Only rows whose `tpep_pickup_datetime` year is 2018 are inserted; other rows (the TLC files contain stray dates) are counted and reported as `rows_outside_year`.
3. A file with zero 2018 rows fails and writes nothing.

Expected input: TLC CSV with this header (the loader checks it and fails clearly if it differs):
`VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount`.
A subset is any chosen months, and `--sample-percent N` keeps a deterministic ~N% of rows. The data files must be placed in GCS by you; the code does not download or substitute data. Files exported from the BigQuery public table use different column names and will be rejected.

Safe reruns: one file per run; the file's `gs://` URI is the rerun key. Each run loads into a temporary staging table (expires in 24 h, dropped afterwards), then one transaction deletes that file's previous rows and inserts the 2018 rows. Rerunning the same file gives the same row count. Schema handling: explicit typed schema, header check, `--max-bad-records 0` by default.

```powershell
# Dry run, prints the plan and SQL, no cloud calls
python -m ingestion.historical --gcs-uri gs://<bucket>/nyc/yellow_tripdata_2018-01.csv --sample-percent 5
# Real load of a 5% sample (after approval, target table created from `python -m ingestion.schemas hist`)
python -m ingestion.historical --gcs-uri gs://<bucket>/nyc/yellow_tripdata_2018-01.csv --sample-percent 5 --execute
```
The historical raw table keeps the TLC columns plus `source_file`, `load_id`, `ingest_ts`. Mapping to the ride-event shape (zone ids, timestamps, fare) is left to the Spark layer.

## Verification queries
- Streaming: [ingestion/sql/verify_streaming.sql](sql/verify_streaming.sql) (row count, duplicates, `ingest_ts`, value ranges, dead-letter contents).
- Historical: [ingestion/sql/verify_historical.sql](sql/verify_historical.sql) (rows per file, year guard, sanity, leftover staging tables).
Run with `bq query --use_legacy_sql=false < ingestion/sql/verify_streaming.sql` or paste into the console after replacing names.

## Safe test sequence
1. Offline: `python -m pytest tests/producer tests/ingestion -q`.
2. Dry runs: `python -m producer --count 10`, `python -m ingestion.historical --gcs-uri ...`.
3. After approval and deployment: `python -m producer --count 10 --duration-seconds 10 --publish`, then run streaming verification query 1 (expect 10 rows).
4. Dead-letter check: `python -m producer --count 10 --duration-seconds 10 --inject-invalid 2 --publish`; expect 8 new rows in `rides_stream` and 2 in the error table.
5. Full run (**needs your explicit approval**): `python -m producer --publish --confirm-full-run` sends 1,000 events over 600 seconds.

## Cloud validations still required
All items are unverified and need your approval before they run.
1. Discovery commands above run and results reviewed (topic, subscription, dataset, buckets, service accounts, template bucket).
2. Template path and parameter names confirmed for `asia-south1` and for the current template version; whether the classic template is still offered or the Flex template should be used.
3. Raw table DDL created; a streaming insert of a `NUMERIC` JSON number and the `DATE(ingest_ts)` partition accepted.
4. UDF executes in the template (ES5/Nashorn); `ingest_ts` populated; invalid events reach the error table. The UDF behaviour tests were skipped locally because Node.js is not installed.
5. 10-event test, dead-letter test, then the 1,000-event run and queries 1 to 5 in `verify_streaming.sql`.
6. IAM bindings approved and applied; least privilege confirmed by a run that needs nothing more.
7. Historical: 2018 file(s) located in GCS, dry run, then real load of a small sample, rerun idempotence check, `verify_historical.sql`. `BigQueryGateway` has never run (it needs real credentials), including transaction/DML on a partitioned table.
8. Dataflow job stopped after the test and cost checked.

## Change requests for the orchestrator
- Paths: this work lives in `producer/`, `ingestion/`, `tests/producer/`, `tests/ingestion/` as instructed by the user, not `src/ingestion/**` as in the agent definition and contract 00. Ownership docs need updating.
- Contract 01 differs from this implementation: 9-field ride schema with `ingest_ts` (not the event-type envelope), topic `rides-events` (not `rh-ride-events`), dataset `raw_ds`, table `rides_stream`, Dataflow template instead of a custom Beam pipeline, `fare` in INR with NUMERIC. Contracts 00, 01 and 03 should be revised or the user's defaults confirmed.
- `.gitignore` should include `.venv/`. Add `HIST_*`, `DATAFLOW_*` and `BQ_*` keys to `.env.example`.
- Terraform should create the topic, subscription, tables, and service account from the names above, only after discovery and approval.

## Known limitations
- `fare` currency is assumed to be INR.
- No `driver_profile` events; driver history is not covered by this stream.
- The producer cannot validate that the topic exists without extra permissions; a missing topic shows up as a failed publish with a readable message.
- Dataflow template behaviour (insert method, dead-letter table schema) is not verified.
