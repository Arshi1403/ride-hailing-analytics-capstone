# Secret Manager integration (example, nothing here is applied)

Composer reads Airflow connections from Secret Manager when the Secret Manager backend is configured. The DAG only refers to
connection ids; it never reads or prints a secret value. Applying any of this needs approval: it changes the environment and IAM.

## 1. Backend configuration (Airflow configuration overrides)
```
secrets/backend        = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend
secrets/backend_kwargs = {"connections_prefix": "airflow-connections", "variables_prefix": null, "sep": "-"}
```
`variables_prefix: null` keeps Airflow Variables in Airflow (no Secret Manager call per Variable read).
```powershell
# JSON contains commas, so choose another list delimiter with the ^#^ prefix
gcloud composer environments update <ENV> --location asia-south1 `
  --update-airflow-configs="^#^secrets-backend=airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend#secrets-backend_kwargs={""connections_prefix"":""airflow-connections"",""variables_prefix"":null,""sep"":""-""}"
```
Check the quoting against the current Composer documentation before running; it is not verified.

## 2. IAM
The environment's service account needs `roles/secretmanager.secretAccessor` on the secrets below (secret level, not project level).

## 3. Alert webhook connection `rh_alert_webhook`
Secret id = prefix + separator + connection id: `airflow-connections-rh_alert_webhook`.
Value (JSON connection; the path token is the secret part of the webhook URL):
```json
{"conn_type": "http", "host": "<webhook host>", "schema": "https", "password": "<path after the host>"}
```
Create the secret and add the value from a file or stdin so it never appears in shell history or logs:
```powershell
gcloud secrets create airflow-connections-rh_alert_webhook --replication-policy=automatic
# paste the JSON, finish with Ctrl+Z then Enter
gcloud secrets versions add airflow-connections-rh_alert_webhook --data-file=-
```
Without this connection alerts are only written to the task log (and e-mail when RH_ALERT_EMAILS is set).

## 4. Rules the code follows
- No secret values in code, environment variables, XCom, logs or alert messages (messages are redacted).
- Google credentials come from the Composer service account through `google_cloud_default`, not from key files.
