# Deliverables checklist (against 05_Capstone_Project.docx)

Last updated 2026-10-15. Evidence types: **LOCAL** = local test or file in the repo; **CLOUD** = real GCP output (none exists yet); **MANUAL** = a step only you can do. "Complete" means the deliverable exists and is proven as far as local evidence allows. Nothing is marked cloud-verified.

## Required deliverables
| # | Deliverable | Status | Evidence / what is left |
|---|---|---|---|
| 1 | `dags/` DAG code + tests | Complete (local) | `dags/ride_hailing_daily.py`, `tests/dags/` pass. Not imported by real Airflow; not deployed. |
| 2 | `spark/` PySpark modules + unit tests | Complete (local) | `spark/ridespark/`, `tests/spark/` (109 pass, 5 file-I/O tests skipped on Windows). Not run on Dataproc. |
| 3 | `sql/` DDL, MERGE, mart views, BQML | Complete (local) | `sql/`, `tests/sql/` (structure and schema alignment). Not run in BigQuery. |
| 4 | `producer/` mock Pub/Sub producer | Complete (local) | `producer/`, `tests/producer/`. Never published to Pub/Sub. |
| 5 | `terraform/` OR `bootstrap.sh` | Complete (local) | `bootstrap.sh` (dry run default, idempotent, no deletes), `tests/infra/`. Not run. |
| 6 | `README.md` (architecture, run instructions, cost estimate) | Complete | `README.md` (cost is an estimate to verify in the pricing calculator). |
| 7 | Architecture diagram PDF/PNG | Complete | `docs/architecture/diagrams/architecture.png`, `.pdf`, source `architecture.mmd`, generator `build_diagram.py`. |
| 8 | Looker Studio dashboard link (reviewer view access) | **MANUAL, not done** | Steps and sharing checklist: `docs/guides/dashboard.md`. Needs data in `mart_ds`. |
| 9 | Cost report screenshot | **MANUAL, not done** | Queries: `sql/cost/`. Procedure: `docs/guides/dashboard.md` section 4. |
| 10 | 10-minute live demo recording | **MANUAL, not done** | Script and recording checklist: `docs/guides/demo-script.md`; resource map: `docs/guides/gcp-resource-map.md`. |

## Required components
| Brief item | Status | Notes |
|---|---|---|
| Producer: 1000 events / 10 min, topic `rides-events`, 9-field JSON | Complete (local) | publishes only with `--publish --confirm-full-run` |
| Dataflow Google template to `raw_ds.rides_stream` | Code ready, not run | subscription variant, max 2 workers, UDF adds `ingest_ts`; launch command is printed, not executed |
| Historical NYC Taxi 2018 to `raw_ds.rides_history` via Composer load | Code ready, not run | `GCSToBigQueryOperator` + idempotent merge; year guard |
| Spark: clean, dedupe, enrich, derive, SCD2, partitioned Parquet | Complete (local) | path is `curated/rides/pickup_date=...` (brief: `dt=`); zones from lookup CSV |
| Fact: partition `pickup_date`, cluster `pickup_zone, driver_id`; SCD2 MERGE | Code ready, not run | |
| Mart views x3 (+ forecast view) | Code ready, not run | |
| BQML ARIMA_PLUS, top 10 zones, 168 h on `v_zone_demand_hourly` | Code ready, not run | training is billable and disabled until `rh_enable_bqml_retrain=true` |
| DAG tasks (10), retries 2 + backoff, SLA 4 h, Secret Manager, backfill 7 days | Code ready, not run | extra helper tasks documented |
| DQ checks + branch + alerts + notify | Code ready, not run | |
| Looker dashboard (KPIs, 30-day revenue, heatmap, forecast overlay) | **MANUAL** | |
| Cost report via `INFORMATION_SCHEMA.JOBS_BY_PROJECT` | SQL ready, not run | |

## Free-tier guardrails
| Guardrail | Status |
|---|---|
| Composer smallest size, delete after | printed by `bootstrap.sh`; **you** create and delete |
| Serverless Dataproc only | yes |
| Dataflow max 2 workers, provided template | yes (`DATAFLOW_MAX_WORKERS=2`) |
| `maximum_bytes_billed` 10 GB | DAG default `RH_MAX_BYTES_BILLED=10000000000`; also set it on manual `bq` runs |
| GCS lifecycle: delete `raw/` after 14 days | in `bootstrap.sh` (skipped when a bucket already has rules) |
| Budget alerts 50 / 100 USD | printed by `bootstrap.sh`; **you** create (needs billing account id) |

## Rubric self-check
| Criterion (weight) | Strong points | Gaps to close |
|---|---|---|
| Correctness and completeness (30) | every layer has code and local tests | **no end-to-end cloud run for one date yet**: this is the biggest gap |
| Production readiness (25) | retries, idempotent merges, DQ gate, alerts, redaction, Secret Manager, tests | unverified against real Airflow/BigQuery; IAM not applied |
| Cost and performance (20) | partition/cluster, Serverless, byte cap, cost SQL, lifecycle | cost report not captured; forecast training reads a view that scans the fact table |
| Documentation and demo (15) | README, diagram, guides, demo script | recording not made; Looker link missing |
| Code quality (10) | modular packages, naming, ~400 tests | no linter configured (add `ruff`) |

## Cloud verification still required (approval needed for each)
1. `bootstrap.sh` dry run reviewed, then `--apply` (buckets, topic, datasets/tables, service accounts); IAM reviewed, then `--with-iam`.
2. Producer: 10-event test, then the full 1,000 events; Dataflow job started and cancelled; `rides_stream` count checked.
3. Historical CSV uploaded; `rides_history` loaded.
4. Composer environment created; DAG imported without errors; one past date run end to end; second run proves reruns change nothing; 7-day backfill run.
5. BQML retrain approved and run once; forecast validation passes.
6. Looker Studio dashboard, cost screenshots, demo recording.
