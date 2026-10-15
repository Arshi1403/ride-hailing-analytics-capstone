# Task Briefs (issued per stage, not yet delegated)

Common to every brief: read `docs/contracts/` first; stay in owned paths; deliver (1) tests with actual output, (2) changed-file report, (3) unresolved issues and change requests, (4) a label for each claim: LOCAL evidence, CLOUD evidence, or "not verified". No cloud actions without user approval.

## Stage B1: infrastructure-security-agent
Contracts 00, 05. Owned: `infra/**`, `scripts/**`, `.github/workflows/**`, `.gitignore`, `.env.example`, `pyproject.toml`, `Makefile`, `config/env/**`.
Deliver: read-only discovery scripts for buckets, topics, datasets, Dataproc, Composer, service accounts; Terraform for the resources in contract 00 (import-friendly, no destroy); least-privilege IAM per contract 05 with review list; `.env.example` with all keys; Makefile targets (`test`, `lint`, `tf-validate`, `diagram`); diagram export script for `docs/architecture/diagrams/architecture.mmd`; offline CI with secret scan.
Validate: `terraform fmt -check`, `terraform validate -backend=false` output; no wildcard/owner/editor roles; env keys match contract 00.

## Stage B2: ingestion-agent
Contracts 00, 01, 03 (raw). Owned: `src/ingestion/**`, `tests/ingestion/**`, `config/ingestion/**`.
Deliver: modular producer (generation, serialization, publisher interface, dry-run default, invalid-event injection); Beam Dataflow pipeline (parse, validate, route, rejects); historical CSV loader (schemas and idempotent batch load config); raw table schema JSON used by Dataflow (must equal contract 03).
Validate: unit tests, including generated events pass contract 01 validation, injected invalid events land in the rejects path, Beam tests with `TestPipeline` and DirectRunner locally.

## Stage C: spark-processing-agent
Contracts 02, 03. Owned: `src/spark/**`, `tests/spark/**`, `config/spark/**`.
Deliver: `run_curation.py`, transforms for dedup, ride state merge, rules R/D, SCD2; Dataproc Serverless batch config; manifest writer.
Validate: local Spark tests for each rule, SCD2 (new, unchanged, changed, out-of-order, rerun idempotence); output schema equals contract 03 staging tables.

## Stage D: bigquery-warehouse-agent
Contract 03. Owned: `sql/**`, `src/warehouse/**`, `tests/warehouse/**`, `config/warehouse/**`.
Deliver: DDL for all datasets and tables, MERGE, marts views, ARIMA_PLUS training and forecast SQL, DQ SQL Q-001..Q-010 writing to `rh_ops.dq_results`, cost SQL, SQL renderer.
Validate: offline render tests, naming and column consistency tests against contracts and Spark output schemas; `bq query --dry_run` only after user approval.

## Stage E: composer-orchestration-agent
Contract 04. Owned: `dags/**`, `tests/dags/**`, `config/composer/**`.
Deliver: `rh_daily_pipeline`, `rh_historical_load`, shared helpers, DagBag tests (imports, task ids, order, retries, schedule, gate behaviour).
Validate: DAG task args match contract 02 and the SQL file paths exist.

## Stage F: testing-documentation-agent
All contracts and handoffs. Owned: `tests/integration/**`, `tests/e2e/**`, `tests/conftest.py`, `tests/fixtures/**`, `README.md`, `docs/guides/**`, `docs/runbooks/**`, `docs/evidence/**`.
Deliver: interface compatibility tests H1..H7; local end-to-end test; README; troubleshooting guide (`docs/runbooks/troubleshooting.md`); `docs/guides/dashboard.md`; `docs/guides/reviewer-sharing-checklist.md`; `docs/guides/cost-report-screenshots.md`; `docs/guides/demo-script.md` (10 minutes with timings); `docs/guides/recording-checklist.md`; evidence index.
Validate: full pytest output; every documented command run or marked not run.
