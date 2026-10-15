# Ten-minute demo script and recording checklist

Goal: show all five layers working end to end for one date, then the evidence for production readiness and cost. Rehearse once; the whole run must already have finished before recording (Composer and Dataflow are slow).

## Before recording (checklist)
- [ ] One successful `ride_hailing_daily` run exists for a past date; the Dataflow job already ran the 1,000-event test and was cancelled afterwards.
- [ ] Tabs open: Storage (landing, curated), Pub/Sub topic, Dataflow job, BigQuery, Dataproc batches, Composer Airflow UI, Looker Studio, [gcp-resource-map.md](gcp-resource-map.md).
- [ ] BigQuery editor tabs with the demo queries from the resource map saved.
- [ ] Browser zoom 125%, notifications off, no secrets visible (no keys, no webhook, hide Secret Manager values).
- [ ] Screen recorder set to 1080p with microphone check; do a 10-second test recording.
- [ ] The Looker link opens in an incognito window.
- [ ] Cloud cost: Composer environment and Dataflow job are deleted/cancelled after recording.
- [ ] Save the file as `demo_<date>.mp4`, upload where reviewers can view it, and add the link to the README.

## Script (10 minutes)
| Time | Show | Say |
|---|---|---|
| 0:00 | README architecture image | the business goal (06:00 metrics, 7-day forecast, SCD2, DQ gates) and the four services: Pub/Sub, Dataflow, Dataproc Serverless, Composer |
| 1:00 | Producer terminal (dry run, then the real run output if recorded earlier) | 1,000 events over 10 minutes, nine-field schema, validation before publishing, dry run by default |
| 2:00 | Pub/Sub topic, Dataflow job graph, `raw_ds.rides_stream` count and `rides_stream_error_records` | Google template, subscription so no message is lost, UDF adds `ingest_ts`, invalid events go to the error table, max 2 workers |
| 3:00 | Composer graph; open `ride_hailing_daily` | tasks in order, retries 2 with backoff, SLA 4 h, Secret Manager for alerts, backfill parameter |
| 3:30 | `raw_ds.rides_history` query by `source_file` | NYC Taxi 2018 only, safe rerun by source file |
| 4:00 | Dataproc batch page, then curated bucket `rides/pickup_date=...`, `_run_metrics` JSON | Serverless (no clusters), clean/dedupe/enrich/derive, partitioned Parquet, rejected rows with reasons |
| 5:00 | `curated_ds.fact_rides` details tab (partition, cluster), MERGE task log | idempotent MERGE keyed on ride_id and pickup_date, rerun changes nothing |
| 5:45 | `dim_driver_scd2` for one driver | SCD2: closed version, new current version, one current row per driver |
| 6:30 | `ops_ds.dq_results` and the `dq_checks` XCom | row counts, null thresholds, duplicates, negative fares, long trips, missing dimensions; failure routes to the alert branch and stops marts and forecast |
| 7:15 | Looker Studio: KPI tiles, revenue line, hourly heatmap, forecast overlay | each chart and its mart view; forecast = BQML ARIMA_PLUS, top 10 zones, 168 hours |
| 8:30 | Cost report screenshots (daily bytes billed, expensive queries) | 10 GB per-query cap, partition and cluster, Serverless, lifecycle rule; the most expensive query and how to reduce it |
| 9:15 | Tests and rubric slide: `python -m pytest tests -q`, deliverables checklist | tests present, what is verified locally vs in the cloud, known gaps |
| 9:45 | Close | what you would do next (driver snapshot source, IAM hardening, Terraform) |

## If something fails during the demo
- Show the Airflow task log and the alert message: that is the failure path working. Do not rerun billable steps live.
- Keep a pre-recorded fallback clip of the Dataflow and Dataproc screens.

## After recording
- [ ] Delete the Composer environment and verify no streaming Dataflow job is running.
- [ ] Check the billing report once more against the cost screenshot.
