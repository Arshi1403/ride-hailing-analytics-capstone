"""Graph and settings of the DAG. Runs against real Airflow when installed, otherwise against tests/dags/airflow_stub.py."""
from datetime import timedelta

import pytest

from dag_loader import arg, load_dag

EXPECTED_TASKS = {
    "sense_historical_dump", "load_history_to_raw", "merge_history_to_raw", "run_curated_spark",
    "load_staging_rides", "load_staging_rejected", "load_staging_dim",
    "merge_fact", "merge_rejected", "merge_dim_scd2",
    "dq_checks", "branch_on_dq", "alert_dq_failure", "refresh_mart_views",
    "branch_weekly_retrain", "skip_retrain", "build_training_series", "retrain_bqml", "forecast_168h", "validate_forecast",
    "retrain_done", "notify_success",
}

EDGES = [
    ("sense_historical_dump", "load_history_to_raw"), ("load_history_to_raw", "merge_history_to_raw"),
    ("merge_history_to_raw", "run_curated_spark"),
    ("run_curated_spark", "load_staging_rides"), ("run_curated_spark", "load_staging_rejected"),
    ("run_curated_spark", "load_staging_dim"),
    ("load_staging_rides", "merge_fact"), ("merge_fact", "merge_rejected"), ("load_staging_rejected", "merge_rejected"),
    ("load_staging_dim", "merge_dim_scd2"),
    ("merge_rejected", "dq_checks"), ("merge_dim_scd2", "dq_checks"),
    ("dq_checks", "branch_on_dq"), ("branch_on_dq", "refresh_mart_views"), ("branch_on_dq", "alert_dq_failure"),
    ("refresh_mart_views", "branch_weekly_retrain"),
    ("branch_weekly_retrain", "build_training_series"), ("branch_weekly_retrain", "skip_retrain"),
    ("build_training_series", "retrain_bqml"), ("retrain_bqml", "forecast_168h"), ("forecast_168h", "validate_forecast"),
    ("validate_forecast", "retrain_done"), ("skip_retrain", "retrain_done"), ("retrain_done", "notify_success"),
]


@pytest.fixture(scope="module")
def dag():
    return load_dag()


def task(dag, task_id):
    return dag.get_task(task_id)


def test_dag_identity_and_safe_defaults(dag):
    assert dag.dag_id == "ride_hailing_daily"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is True  # nothing runs until someone unpauses it
    assert (getattr(dag, "schedule_interval", None) or dag.schedule) == "0 1 * * *"
    assert dag.dagrun_timeout == timedelta(hours=5)


def test_all_ten_logical_tasks_exist_plus_the_documented_helpers(dag):
    assert set(dag.task_ids) == EXPECTED_TASKS
    for logical in ("sense_historical_dump", "load_history_to_raw", "run_curated_spark", "merge_fact", "merge_dim_scd2",
                    "refresh_mart_views", "retrain_bqml", "dq_checks", "branch_on_dq", "notify_success"):
        assert logical in dag.task_ids


@pytest.mark.parametrize("upstream,downstream", EDGES)
def test_dependency_exists(dag, upstream, downstream):
    assert downstream in task(dag, upstream).downstream_task_ids
    assert upstream in task(dag, downstream).upstream_task_ids


def test_no_unexpected_edges(dag):
    actual = {(t.task_id, d) for t in dag.tasks for d in t.downstream_task_ids}
    assert actual == set(EDGES)


def test_data_quality_gates_marts_forecast_and_success(dag):
    def reachable(start):
        seen, stack = set(), [start]
        while stack:
            for nxt in task(dag, stack.pop()).downstream_task_ids:
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        return seen

    after_failure_path = reachable("alert_dq_failure")
    assert after_failure_path == set()  # the alert branch is a dead end: success, marts and forecast are never reached
    after_pass = reachable("refresh_mart_views")
    assert {"build_training_series", "retrain_bqml", "forecast_168h", "notify_success"} <= after_pass
    assert "dq_checks" in task(dag, "branch_on_dq").upstream_task_ids


def test_marts_forecast_and_notification_are_downstream_of_the_dq_gate(dag):
    gate = task(dag, "branch_on_dq")
    for name in ("refresh_mart_views", "build_training_series", "retrain_bqml", "forecast_168h", "notify_success"):
        seen, stack = set(), [name]
        while stack:
            for up in task(dag, stack.pop()).upstream_task_ids:
                if up not in seen:
                    seen.add(up)
                    stack.append(up)
        assert gate.task_id in seen, name


def test_two_retries_with_exponential_backoff_on_work_tasks(dag):
    exempt = {"sense_historical_dump", "alert_dq_failure"}  # documented: waiting budget / deterministic alert
    for t in dag.tasks:
        if t.task_id in exempt:
            assert t.retries == 0
            continue
        assert t.retries == 2, t.task_id
        assert t.retry_exponential_backoff is True, t.task_id


def test_four_hour_sla_and_practical_timeouts(dag):
    for t in dag.tasks:
        assert t.sla == timedelta(hours=4), t.task_id
        assert t.execution_timeout is not None and t.execution_timeout <= timedelta(hours=3), t.task_id
    assert task(dag, "run_curated_spark").execution_timeout == timedelta(hours=2, minutes=15)
    assert arg(task(dag, "sense_historical_dump"), "timeout") == 3600


def test_trigger_rules_make_branches_and_optional_steps_safe(dag):
    rule = lambda name: task(dag, name).trigger_rule  # noqa: E731
    assert rule("run_curated_spark") == "none_failed"  # optional historical dump
    assert rule("dq_checks") == "none_failed"  # optional SCD2 merge
    assert rule("retrain_done") == "none_failed_min_one_success"
    assert rule("notify_success") == "none_failed_min_one_success"


def test_historical_dump_is_waited_for_with_a_reschedule_sensor(dag):
    sensor = task(dag, "sense_historical_dump")
    assert arg(sensor, "mode") == "reschedule" and arg(sensor, "bucket") == "rh-landing-test"
    assert arg(sensor, "object") == "{{ rh_historical_object(ds, params) }}"
    assert arg(sensor, "soft_fail") is False
    assert arg(load_dag(RH_HISTORICAL_DUMP_REQUIRED="false").get_task("sense_historical_dump"), "soft_fail") is True


def test_spark_task_is_a_dataproc_serverless_batch(dag):
    spark_task = task(dag, "run_curated_spark")
    assert type(spark_task).__name__ == "DataprocCreateBatchOperator"
    assert arg(spark_task, "region") == "asia-south1" and arg(spark_task, "batch")["runtime_config"]["version"] == "2.2"
    batch_id = arg(spark_task, "batch_id")
    assert "{{ ti.try_number }}" in batch_id and "ts_nodash" in batch_id
    assert arg(spark_task, "do_xcom_push") is False


def test_sql_tasks_run_idempotent_scripts_with_a_byte_cap(dag):
    for name in ("merge_fact", "merge_rejected", "merge_dim_scd2", "refresh_mart_views", "build_training_series",
                 "retrain_bqml", "forecast_168h", "merge_history_to_raw"):
        operator = task(dag, name)
        query = arg(operator, "configuration")["query"]
        assert query["useLegacySql"] is False and int(query["maximumBytesBilled"]) > 0
        assert arg(operator, "force_rerun") is True and arg(operator, "location") == "asia-south1"
        assert arg(operator, "do_xcom_push") is False


def test_history_is_loaded_with_gcs_to_bigquery_into_a_replaced_staging_table(dag):
    load = task(dag, "load_history_to_raw")
    assert type(load).__name__ == "GCSToBigQueryOperator"
    assert arg(load, "write_disposition") == "WRITE_TRUNCATE" and arg(load, "skip_leading_rows") == 1
    assert arg(load, "destination_project_dataset_table") == "labs-507309.raw_ds.rides_history_stg"
    assert arg(load, "source_objects") == ["{{ rh_historical_object(ds, params) }}"]


def test_backfill_parameter_is_capped_at_seven_days(dag):
    param = dag.params.get_param("backfill_days") if hasattr(dag.params, "get_param") else dag.params["backfill_days"]
    schema = param.schema if isinstance(getattr(param, "schema", None), dict) else {}
    assert schema.get("maximum") == 7 and schema.get("minimum") == 0
    assert set(dag.params) == {"backfill_days", "force_retrain", "historical_object"}


def test_template_search_path_and_macros_are_configured(dag):
    assert dag.template_searchpath == ["/home/airflow/gcs/data/sql"]
    assert {"rh_window_start", "rh_run_id", "rh_historical_object", "rh_as_of_ts"} <= set(dag.user_defined_macros)


def test_alert_callbacks_and_email_defaults(dag):
    assert callable(dag.default_args["on_failure_callback"]) and callable(dag.on_failure_callback)
    assert callable(dag.sla_miss_callback)
    assert dag.default_args["email_on_failure"] is False  # no RH_ALERT_EMAILS in the test environment
    with_mail = load_dag(RH_ALERT_EMAILS="ops@example.com")
    assert with_mail.default_args["email_on_failure"] is True and with_mail.default_args["email"] == ["ops@example.com"]


def test_dag_does_not_import_without_required_settings():
    with pytest.raises(Exception, match="RH_LANDING_BUCKET") as info:
        load_dag(env={"GCP_PROJECT_ID": "labs-507309"})
    assert type(info.value).__name__ == "ConfigError"
