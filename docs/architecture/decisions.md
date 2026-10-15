# Decision Log

| ID | Status | Context | Decision | Consequences |
|---|---|---|---|---|
| D-001 | Decided | User requires the checklist at `docs/DELIVERABLES_CHECKLIST.md`; the agent files named `docs/requirements/checklist.md` | Single checklist at `docs/DELIVERABLES_CHECKLIST.md`, owned by the orchestrator | Orchestrator agent file updated; no second checklist |
| D-002 | Decided | SCD2 and idempotent loads | Spark computes SCD2 change rows (CLOSE/OPEN) by reading current `rh_curated.dim_driver_scd2`; BigQuery MERGE applies them | Spark depends on the BigQuery connector; MERGE is rerunnable |
| D-003 | Decided | Bad-data prevention | Three gates: Dataflow, Spark, BigQuery DQ. Marts read curated only | DAG must fail on ERROR checks before marts and forecast |
| D-004 | Decided | Parquet to BigQuery curated | Composer loads Parquet to `rh_staging` (truncate per run), then MERGE | Extra dataset `rh_staging` |
| D-005 | Decided | Streaming raw handling | One topic, `event_type` routing to two raw tables | Single Dataflow job |
| D-017 | Decided | Out-of-order driver events | Rejected with rule D-005 (older than current version) | Late driver history needs the historical CSV path |
| D-006 | ASSUMPTION | Business timezone | IST (`Asia/Kolkata`) defines business day and the 06:00 deadline | Confirm |
| D-007 | ASSUMPTION | Currency | INR only; other currency is rejected | Confirm |
| D-008 | ASSUMPTION | Driver changes source | `driver_profile_updated` stream events plus `hist_drivers` CSV. SCD2 tracked attributes: city, vehicle_type, status, home_zone_id, rating_band | Confirm |
| D-009 | ASSUMPTION | Zone reference | Zone list supplied as CSV `reference/zones.csv` in the landing bucket; producer uses a bundled copy | Confirm; user may need to provide real zones |
| D-010 | ASSUMPTION | BigQuery and ARIMA_PLUS in `asia-south1` | Dataset location `asia-south1`; ARIMA_PLUS availability there is not verified | Fallback if unsupported: change `BQ_LOCATION` to a supported location (for example multi-region `US`) after discovery |
| D-011 | ASSUMPTION | Forecast scope | Daily grain, 7-day horizon, zones with at least 30 days history; others skipped and reported in DQ | Confirm |
| D-012 | ASSUMPTION | Retraining | `CREATE OR REPLACE MODEL` on the pipeline-owned model `rh_ml.demand_arima_model` each day. This replaces our own object only, but needs approval the first time it is enabled | Billable; gated |
| D-013 | ASSUMPTION | Dataproc Serverless runtime | Runtime 2.2 (Spark 3.5, Python 3.11) with the spark-bigquery connector | Verify during discovery |
| D-014 | ASSUMPTION | Composer version | Composer 2 / Airflow 2.x; Composer environment exists or is created by Terraform only after approval | Discovery first |
| D-015 | Decided | Diagram export | Source in Mermaid; PNG/PDF produced by a script from infrastructure-security-agent using mermaid-cli | Needs Node.js locally; not yet exported |
| D-016 | Decided | Ownership additions | Dataflow code under `src/ingestion/dataflow/`; dashboard, demo, cost and reviewer docs under `docs/guides/`; screenshots under `docs/evidence/` (testing-documentation-agent) | Agent files updated |
