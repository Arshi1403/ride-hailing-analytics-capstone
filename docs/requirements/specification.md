# Project Specification (recorded verbatim from the user, 2026-10-15)

Ride-Hailing Analytics Platform capstone. New project, built from scratch in this workspace. No previous repository is used.

## Business requirements
- Provide daily operational metrics by 06:00.
- Forecast demand for the next seven days by pickup zone.
- Maintain driver history using SCD Type 2.
- Prevent bad data from reaching analytics and dashboards.

## Required architecture
- Python producer -> Pub/Sub -> Dataflow -> BigQuery raw.
- Historical CSV from GCS -> BigQuery raw.
- BigQuery raw -> Dataproc Serverless/PySpark -> curated Parquet and BigQuery curated tables.
- BigQuery curated -> mart views and BQML ARIMA_PLUS forecasting -> Looker Studio.
- Cloud Composer orchestrates the daily workflow.

## Required deliverables
- Modular producer and ingestion code.
- PySpark processing and SCD2 implementation.
- BigQuery DDL, MERGE statements, mart views, forecasting SQL, DQ SQL and cost SQL.
- Composer DAGs and tests.
- Terraform or bootstrap scripts.
- README and troubleshooting guide.
- Architecture diagram source and PNG/PDF export.
- Dashboard instructions and reviewer-sharing checklist.
- Cost-report screenshot procedure.
- Ten-minute demo script and recording checklist.

## Responsibilities
1. Maintain `docs/DELIVERABLES_CHECKLIST.md`.
2. Define schemas, paths, dataset naming, configuration variables, and component interfaces before implementation.
3. Assign distinct file ownership to specialist agents.
4. Track dependencies and resolve incompatible interfaces.
5. Require each specialist to provide tests, changed-file reports, and unresolved issues.
6. Coordinate integration and run a final acceptance review.

## Implementation order
A. Requirements, architecture and contracts.
B. Infrastructure definitions and ingestion.
C. Spark processing and SCD2.
D. BigQuery warehouse and forecasting.
E. Composer orchestration.
F. Testing, integration, documentation and evidence.

## Safety rules
- Do not access the old repository.
- Do not delete existing GCP resources.
- Require approval for billable jobs, deployments, IAM changes and destructive operations.
- Distinguish local test results from actual cloud verification.
- Never mark a deliverable complete without evidence.

## Configuration defaults (from earlier instructions)
- GCP project `labs-507309`, region `asia-south1`. Defaults only, not proof that resources exist.
