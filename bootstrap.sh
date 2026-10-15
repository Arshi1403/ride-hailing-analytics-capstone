#!/usr/bin/env bash
# Bootstrap for the ride-hailing capstone: buckets, Pub/Sub, BigQuery datasets and tables, service accounts.
#
# DRY RUN BY DEFAULT: prints every command and changes nothing. Add --apply to create resources.
# Safe to rerun: existing buckets, topics, subscriptions and service accounts are detected and left alone.
# It never deletes, overwrites or recreates anything, and it never changes IAM unless you also pass --with-iam.
# Run it in Cloud Shell or Git Bash after `gcloud auth login`. Composer and budgets are only printed (cost/billing decisions).
#
# Usage: ./bootstrap.sh [--apply] [--with-iam] [--upload-code]
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-labs-507309}"
REGION="${GCP_REGION:-asia-south1}"
LANDING="${RH_LANDING_BUCKET:-${PROJECT_ID}-rh-landing}"
CURATED="${RH_CURATED_BUCKET:-${PROJECT_ID}-rh-curated}"
ARTIFACTS="${RH_ARTIFACTS_BUCKET:-${PROJECT_ID}-rh-artifacts}"
TOPIC="${PUBSUB_TOPIC:-rides-events}"
SUBSCRIPTION="${PUBSUB_SUBSCRIPTION:-rides-events-sub}"
RAW_RETENTION_DAYS=14

APPLY=false
WITH_IAM=false
UPLOAD_CODE=false
for arg in "$@"; do
  case "$arg" in
    --apply) APPLY=true ;;
    --with-iam) WITH_IAM=true ;;
    --upload-code) UPLOAD_CODE=true ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

cd "$(dirname "$0")"
MODE="DRY RUN (nothing is changed; add --apply)"
$APPLY && MODE="APPLY"
echo "== ride-hailing bootstrap: project=${PROJECT_ID} region=${REGION} mode=${MODE}"

run() {  # print, and execute only with --apply
  echo "+ $*"
  if $APPLY; then "$@"; fi
}

step() { echo; echo "-- $*"; }

bucket_exists() { gcloud storage buckets describe "gs://$1" --project "$PROJECT_ID" >/dev/null 2>&1; }

ensure_bucket() {
  local name="$1"
  if $APPLY && bucket_exists "$name"; then
    echo "exists: gs://${name} (left unchanged)"
  else
    run gcloud storage buckets create "gs://${name}" --project "$PROJECT_ID" --location "$REGION" --uniform-bucket-level-access
  fi
}

ensure_lifecycle() {  # expire objects under a prefix after N days; skipped when the bucket already has lifecycle rules
  local name="$1" prefix="$2" file
  file="$(mktemp)"
  printf '{"rule":[{"action":{"type":"Delete"},"condition":{"age":%s,"matchesPrefix":["%s"]}}]}\n' "$RAW_RETENTION_DAYS" "$prefix" > "$file"
  if $APPLY && [ -n "$(gcloud storage buckets describe "gs://${name}" --format='value(lifecycle_config)' 2>/dev/null)" ]; then
    echo "gs://${name} already has lifecycle rules: not replaced. Add '${prefix}' after ${RAW_RETENTION_DAYS} days by hand."
  else
    run gcloud storage buckets update "gs://${name}" --lifecycle-file="$file"
  fi
}

step "1. APIs"
run gcloud services enable storage.googleapis.com pubsub.googleapis.com dataflow.googleapis.com dataproc.googleapis.com \
  bigquery.googleapis.com composer.googleapis.com secretmanager.googleapis.com --project "$PROJECT_ID"

step "2. Buckets (landing: raw/ historical/ reference/, curated: Spark Parquet, artifacts: code and Dataflow temp)"
ensure_bucket "$LANDING"
ensure_bucket "$CURATED"
ensure_bucket "$ARTIFACTS"
ensure_lifecycle "$LANDING" "raw/"
ensure_lifecycle "$ARTIFACTS" "dataflow/"

step "3. Pub/Sub topic and subscription (the Dataflow template reads the subscription)"
if $APPLY && gcloud pubsub topics describe "$TOPIC" --project "$PROJECT_ID" >/dev/null 2>&1; then
  echo "exists: topic ${TOPIC}"
else
  run gcloud pubsub topics create "$TOPIC" --project "$PROJECT_ID"
fi
if $APPLY && gcloud pubsub subscriptions describe "$SUBSCRIPTION" --project "$PROJECT_ID" >/dev/null 2>&1; then
  echo "exists: subscription ${SUBSCRIPTION}"
else
  run gcloud pubsub subscriptions create "$SUBSCRIPTION" --topic "$TOPIC" --project "$PROJECT_ID" \
    --ack-deadline=60 --message-retention-duration=7d --expiration-period=never
fi

step "4. BigQuery datasets and tables (CREATE ... IF NOT EXISTS, in order)"
for file in sql/ddl/*.sql; do
  echo "+ bq query < ${file}"
  if $APPLY; then
    bq query --use_legacy_sql=false --project_id="$PROJECT_ID" --location="$REGION" < "$file"
  fi
done

step "5. Service accounts (created only if missing)"
for sa in sa-rh-dataflow sa-rh-spark sa-rh-composer; do
  if $APPLY && gcloud iam service-accounts describe "${sa}@${PROJECT_ID}.iam.gserviceaccount.com" --project "$PROJECT_ID" >/dev/null 2>&1; then
    echo "exists: ${sa}"
  else
    run gcloud iam service-accounts create "$sa" --project "$PROJECT_ID" --display-name "ride-hailing ${sa#sa-rh-}"
  fi
done

step "6. IAM (least privilege; applied only with --with-iam, otherwise printed for review)"
bind_project() { run gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$1@${PROJECT_ID}.iam.gserviceaccount.com" --role="$2" --condition=None; }
bind_bucket() { run gcloud storage buckets add-iam-policy-binding "gs://$1" --member="serviceAccount:$2@${PROJECT_ID}.iam.gserviceaccount.com" --role="$3"; }
bind_dataset() { echo "+ grant $3 on dataset $1 to $2 (bq update --source with an access entry; review in the console or use Terraform)"; }
iam() {
  if $WITH_IAM && $APPLY; then "$@"; else echo "(review only) $*"; fi
}
iam bind_project sa-rh-dataflow roles/dataflow.worker
iam bind_project sa-rh-dataflow roles/bigquery.jobUser
iam bind_project sa-rh-spark roles/dataproc.worker
iam bind_project sa-rh-spark roles/bigquery.jobUser
iam bind_project sa-rh-spark roles/bigquery.readSessionUser
iam bind_project sa-rh-composer roles/composer.worker
iam bind_project sa-rh-composer roles/bigquery.jobUser
iam bind_project sa-rh-composer roles/dataproc.editor
iam bind_bucket "$ARTIFACTS" sa-rh-dataflow roles/storage.objectAdmin
iam bind_bucket "$CURATED" sa-rh-spark roles/storage.objectAdmin
iam bind_bucket "$ARTIFACTS" sa-rh-spark roles/storage.objectAdmin
iam bind_bucket "$LANDING" sa-rh-spark roles/storage.objectViewer
iam bind_bucket "$LANDING" sa-rh-composer roles/storage.objectViewer
iam bind_bucket "$CURATED" sa-rh-composer roles/storage.objectViewer
bind_dataset raw_ds sa-rh-dataflow roles/bigquery.dataEditor
bind_dataset raw_ds sa-rh-spark roles/bigquery.dataViewer
bind_dataset curated_ds sa-rh-spark roles/bigquery.dataViewer
echo "Dataset-level roles and 'serviceAccountUser on sa-rh-spark for sa-rh-composer' are listed in docs/contracts/05-iam-requests.md; apply them after review."

if $UPLOAD_CODE; then
  step "7. Upload Spark code, DAG SQL and the example zone lookup location (overwrites objects with the same name)"
  run python spark/build_package.py
  run gcloud storage cp spark/run_job.py spark/dist/ridespark.zip "gs://${ARTIFACTS}/spark/code/"
  run gcloud storage cp ingestion/udf/ride_event_udf.js "gs://${ARTIFACTS}/dataflow/udf/"
fi

step "8. Printed only (cost and billing decisions, not run by this script)"
cat <<EOF
Composer (smallest size; delete it after the capstone):
  gcloud composer environments create ride-hailing-capstone --location ${REGION} --image-version composer-2-airflow-2 \\
    --environment-size small --service-account sa-rh-composer@${PROJECT_ID}.iam.gserviceaccount.com --project ${PROJECT_ID}
Dataflow (max 2 workers, Google template): python -m ingestion.dataflow   # prints the command, review, then run it
Budget alerts at 50 and 100 USD (needs your billing account id):
  gcloud billing budgets create --billing-account=BILLING_ACCOUNT_ID --display-name=capstone-50 --budget-amount=50USD --threshold-rule=percent=1.0
  gcloud billing budgets create --billing-account=BILLING_ACCOUNT_ID --display-name=capstone-100 --budget-amount=100USD --threshold-rule=percent=1.0
EOF

echo
echo "== done (${MODE})"
