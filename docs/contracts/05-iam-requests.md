# Contract 05: Service Accounts and IAM Requests (v0.1.0, draft)

These are requests for infrastructure-security-agent to encode in Terraform. Nothing is applied until the user approves each binding. No owner, editor or wildcard roles.

| Service account | Roles (scope) | Why |
|---|---|---|
| `sa-rh-producer` | `roles/pubsub.publisher` on topic `rh-ride-events` | Publish events |
| `sa-rh-dataflow` | `roles/dataflow.worker` (project), `roles/pubsub.subscriber` on subscription, `roles/bigquery.dataEditor` on `rh_raw`, `roles/bigquery.jobUser` (project), `roles/storage.objectAdmin` on artifacts bucket | Streaming job |
| `sa-rh-spark` | `roles/dataproc.worker` (project), `roles/bigquery.dataViewer` on `rh_raw` and `rh_curated`, `roles/bigquery.readSessionUser` (project), `roles/bigquery.jobUser`, `roles/storage.objectAdmin` on curated and artifacts buckets, `roles/storage.objectViewer` on landing | Batch job |
| `sa-rh-composer` | `roles/composer.worker`, `roles/dataproc.editor` (project), `roles/bigquery.dataEditor` on `rh_raw`, `rh_staging`, `rh_curated`, `rh_marts`, `rh_ml`, `rh_ops`, `roles/bigquery.jobUser`, `roles/storage.objectAdmin` on curated, `roles/storage.objectViewer` on landing, `roles/iam.serviceAccountUser` on `sa-rh-spark` | Orchestration |
| Looker Studio viewer (user or group) | `roles/bigquery.dataViewer` on `rh_marts` and `rh_ml` forecast view dependencies, `roles/bigquery.jobUser` | Dashboard |

Note on views: authorized views are preferred so viewers need no access to `rh_curated` (decision for bigquery-warehouse-agent and infrastructure to confirm in Stage B/D).
Also requested: Pub/Sub dead-letter topic and subscription, bucket lifecycle rules (temp, 30 days), budget alert suggestion.
