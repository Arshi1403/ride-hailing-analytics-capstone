# Implementation Order

Rules: one stage at a time. A stage is done when its checklist items are `implemented` with LOCAL evidence and the cross-agent interface check passes. Cloud steps need explicit user approval and are tracked separately as CLD-xxx.

| Stage | Scope | Agents (in order) | Exit criteria |
|---|---|---|---|
| A | Spec, architecture, contracts, checklist | orchestrator | User confirms ASSUMPTIONs |
| B | Infrastructure definitions and ingestion | infrastructure-security, then ingestion | `terraform validate` output; producer and Dataflow unit tests pass; discovery scripts written (not necessarily run) |
| C | Spark processing and SCD2 | spark-processing | Local Spark tests pass; output schemas equal contract 02 |
| D | BigQuery warehouse and forecasting | bigquery-warehouse | SQL render tests pass; DDL matches contract 03 and Spark output; `--dry_run` only if approved |
| E | Composer orchestration | composer-orchestration | DagBag test passes; task args match contract 04 |
| F | Integration, docs, evidence | testing-documentation, then orchestrator acceptance | Full local pytest output; docs label verified vs not verified |

Per-stage handoff (required from every specialist): changed-file report, test output, unresolved issues, change requests.

Approvals needed (none requested yet): discovery commands against GCP (read-only, needs authenticated gcloud), Terraform plan, any apply, IAM binding, Dataflow/Dataproc/Composer runs, producer in non-dry-run mode, BQML training.
