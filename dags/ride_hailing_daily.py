"""ride_hailing_daily: daily ride-hailing analytics pipeline for Cloud Composer (Airflow 2.4+).

Flow: wait for historical CSV -> load to raw -> Dataproc Serverless Spark -> load Parquet to staging -> MERGE fact and
SCD2 dimension -> data-quality gate -> marts -> weekly BQML retrain and 168-hour forecast -> notification.
See dags/README.md for configuration, backfill and the deployment runbook. The DAG is created paused.
Parse time does no network calls: configuration comes from environment variables, everything else resolves at run time.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.trigger_rule import TriggerRule

from common import alerts, historical_sql, spark_batch, sql_jobs, tasks
from common.config import load_settings
from common.windows import MAX_BACKFILL_DAYS, make_macros

settings = load_settings()
on_task_failure, on_dag_failure, on_sla_miss = alerts.make_callbacks(settings.alert_conn_id)

DEFAULT_ARGS = {
    "owner": "ride-hailing-capstone",
    "depends_on_past": False,  # every run recomputes its own window, so a failed day must not block the next one
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,  # 5 min, then about 10 min, capped below
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=45),  # per attempt; heavier tasks override it
    "sla": timedelta(hours=4),  # end-to-end target, measured from the scheduled run time
    "email": list(settings.alert_emails),
    "email_on_failure": bool(settings.alert_emails),
    "email_on_retry": False,
    "on_failure_callback": on_task_failure,
}

DOC = """
### ride_hailing_daily
Runs daily for the previous business day. Window of pickup dates recomputed: `ds - lookback` .. `ds`
(or the last N dates when the run is triggered with `{"backfill_days": N}`, N up to 7).
Every step is idempotent: rerunning a task or a whole run gives the same result.
Data-quality failures stop the marts and the forecast, alert, and fail the run.
The weekly model retrain only runs on the retrain weekday and only when Airflow Variable `rh_enable_bqml_retrain` is `true`.
"""


def bq_job(job: sql_jobs.SqlJob, **overrides) -> BigQueryInsertJobOperator:
    args = dict(
        task_id=job.task_id,
        configuration=sql_jobs.query_configuration(job, settings),
        location=settings.bq_location,
        project_id=settings.project_id,
        gcp_conn_id=settings.gcp_conn_id,
        force_rerun=True,  # a retry or clear must run the (idempotent) SQL again, not re-attach to a finished job
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=30),
    )
    args.update(overrides)
    return BigQueryInsertJobOperator(**args)


with DAG(
    dag_id=sql_jobs.DAG_ID,
    description="Daily ride-hailing analytics: ingestion check, Spark curation, BigQuery merges, DQ gate, marts, weekly forecast",
    schedule=settings.schedule_cron,
    start_date=pendulum.datetime(2026, 10, 1, tz=settings.schedule_tz),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=5),
    is_paused_upon_creation=True,
    params={
        "backfill_days": Param(0, type="integer", minimum=0, maximum=MAX_BACKFILL_DAYS,
                               description="0 = normal daily window; 1..7 = recompute that many pickup dates ending at the run date"),
        "force_retrain": Param(False, type="boolean", description="retrain the model on this run (still needs rh_enable_bqml_retrain=true)"),
        "historical_object": Param("", type="string", description="override the expected historical object path inside the landing bucket"),
    },
    user_defined_macros=make_macros(settings),
    template_searchpath=[settings.sql_dir],
    sla_miss_callback=on_sla_miss,
    on_failure_callback=on_dag_failure,
    tags=["ride-hailing", "capstone"],
    doc_md=DOC,
) as dag:

    sense_historical_dump = GCSObjectExistenceSensor(
        task_id="sense_historical_dump",
        bucket=settings.landing_bucket,
        object="{{ rh_historical_object(ds, params) }}",
        google_cloud_conn_id=settings.gcp_conn_id,
        mode="reschedule",
        poke_interval=300,
        timeout=3600,
        soft_fail=not settings.historical_required,  # optional dump: skip the load instead of failing the run
        retries=0,  # the sensor timeout is the wait budget; retrying would multiply it
        execution_timeout=timedelta(minutes=10),
    )

    load_history_to_raw = GCSToBigQueryOperator(
        task_id="load_history_to_raw",
        bucket=settings.landing_bucket,
        source_objects=[historical_sql.object_template()],
        destination_project_dataset_table=f"{settings.project_id}.{historical_sql.STAGING_TABLE}",
        schema_fields=historical_sql.schema_fields(),
        source_format="CSV",
        skip_leading_rows=1,
        max_bad_records=0,
        write_disposition="WRITE_TRUNCATE",  # staging table is replaced on every run, so reruns are safe
        create_disposition="CREATE_IF_NEEDED",
        location=settings.bq_location,
        gcp_conn_id=settings.gcp_conn_id,
        execution_timeout=timedelta(minutes=45),
    )

    merge_history_to_raw = BigQueryInsertJobOperator(
        task_id="merge_history_to_raw",
        configuration=historical_sql.merge_configuration(settings),
        location=settings.bq_location,
        project_id=settings.project_id,
        gcp_conn_id=settings.gcp_conn_id,
        force_rerun=True,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=30),
    )

    run_curated_spark = DataprocCreateBatchOperator(
        task_id="run_curated_spark",
        project_id=settings.project_id,
        region=settings.region,
        batch=spark_batch.batch(settings),
        batch_id=spark_batch.batch_id_template(),
        gcp_conn_id=settings.gcp_conn_id,
        timeout=7200,
        execution_timeout=timedelta(hours=2, minutes=15),
        trigger_rule=TriggerRule.NONE_FAILED,  # runs when the historical dump was skipped (optional dump)
        do_xcom_push=False,
    )

    staging = {
        key: PythonOperator(task_id=key, python_callable=tasks.load_staging, op_kwargs={"task_key": key})
        for key in ("load_staging_rides", "load_staging_rejected", "load_staging_dim")
    }

    merge_fact = bq_job(sql_jobs.JOBS["merge_fact"])
    merge_rejected = bq_job(sql_jobs.JOBS["merge_rejected"])
    merge_dim_scd2 = bq_job(sql_jobs.JOBS["merge_dim_scd2"])

    dq_checks = PythonOperator(
        task_id="dq_checks",
        python_callable=tasks.dq_checks,
        trigger_rule=TriggerRule.NONE_FAILED,  # merge_dim_scd2 may be skipped when no driver snapshot is configured
    )

    branch_on_dq = BranchPythonOperator(task_id="branch_on_dq", python_callable=tasks.branch_on_dq)

    alert_dq_failure = PythonOperator(
        task_id="alert_dq_failure",
        python_callable=tasks.alert_dq_failure,
        retries=0,  # deterministic: the alert is sent once and the run is failed on purpose
        execution_timeout=timedelta(minutes=5),
    )

    refresh_mart_views = bq_job(sql_jobs.JOBS["refresh_mart_views"], execution_timeout=timedelta(minutes=15))

    branch_weekly_retrain = BranchPythonOperator(task_id="branch_weekly_retrain", python_callable=tasks.branch_weekly_retrain)
    skip_retrain = EmptyOperator(task_id="skip_retrain")
    build_training_series = bq_job(sql_jobs.JOBS["build_training_series"])
    retrain_bqml = bq_job(sql_jobs.JOBS["retrain_bqml"], execution_timeout=timedelta(minutes=90))
    forecast_168h = bq_job(sql_jobs.JOBS["forecast_168h"])
    validate_forecast = PythonOperator(
        task_id="validate_forecast", python_callable=tasks.validate_forecast, execution_timeout=timedelta(minutes=15)
    )
    retrain_done = EmptyOperator(task_id="retrain_done", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=tasks.notify_success,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        execution_timeout=timedelta(minutes=10),
    )

    sense_historical_dump >> load_history_to_raw >> merge_history_to_raw >> run_curated_spark
    run_curated_spark >> [staging["load_staging_rides"], staging["load_staging_rejected"], staging["load_staging_dim"]]
    staging["load_staging_rides"] >> merge_fact
    [merge_fact, staging["load_staging_rejected"]] >> merge_rejected
    staging["load_staging_dim"] >> merge_dim_scd2
    [merge_rejected, merge_dim_scd2] >> dq_checks >> branch_on_dq
    branch_on_dq >> [refresh_mart_views, alert_dq_failure]
    refresh_mart_views >> branch_weekly_retrain >> [build_training_series, skip_retrain]
    build_training_series >> retrain_bqml >> forecast_168h >> validate_forecast >> retrain_done
    skip_retrain >> retrain_done
    retrain_done >> notify_success
