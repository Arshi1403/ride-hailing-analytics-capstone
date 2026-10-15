"""Interface checks between the DAG and the other agents' work: SQL parameters, Spark arguments, schemas, staging layout."""
import re
import sys

import pytest

import dag_loader  # noqa: F401
from common import dq, historical_sql, spark_batch, sql_jobs
from common.config import load_settings
from dag_loader import DAGS, ENV, ROOT

SQL = ROOT / "sql"
SETTINGS = load_settings({**ENV, "RH_DRIVER_SNAPSHOT_URI": "gs://rh-landing-test/drivers/snapshot.parquet",
                          "RH_ALLOW_MISSING_DIMENSIONS": "true"})


def sql_text(relative):
    return (SQL / relative).read_text(encoding="utf-8")


@pytest.mark.parametrize("job", list(sql_jobs.JOBS.values()), ids=list(sql_jobs.JOBS))
def test_sql_job_parameters_equal_the_parameters_the_sql_uses(job):
    used = set()
    for file in job.files:
        assert (SQL / file).is_file(), f"{file} is missing from sql/"
        used |= set(dq.params_in_sql(sql_text(file)))
    assert set(job.params) == used


def test_every_parameter_has_a_bigquery_type_and_a_value_template():
    templates = sql_jobs.param_templates(SETTINGS)
    for job in sql_jobs.JOBS.values():
        for name in job.params:
            assert name in sql_jobs.PARAM_TYPES and name in templates, name


def test_dq_files_exist_and_every_parameter_is_known():
    for file in (*sql_jobs.DQ_FILES, sql_jobs.FORECAST_VALIDATION_FILE):
        for name in dq.params_in_sql(sql_text(file)):
            assert name in sql_jobs.PARAM_TYPES, f"{file} uses @{name}, which has no declared type"


def test_all_dq_sql_files_of_the_warehouse_agent_are_executed():
    on_disk = {f"data_quality/{p.name}" for p in (SQL / "data_quality").glob("*.sql")}
    assert on_disk == set(sql_jobs.DQ_FILES)


def test_sql_files_contain_no_jinja_delimiters_because_they_are_rendered_by_include():
    for path in SQL.rglob("*.sql"):
        text = path.read_text(encoding="utf-8")
        assert "{{" not in text and "{%" not in text and "{#" not in text, path.name


def test_query_configuration_renders_named_typed_parameters():
    config = sql_jobs.query_configuration(sql_jobs.JOBS["merge_fact"], SETTINGS)
    query = config["query"]
    assert query["query"] == "{% include 'merge/merge_fact_rides.sql' %}"
    assert query["parameterMode"] == "NAMED"
    by_name = {p["name"]: p for p in query["queryParameters"]}
    assert by_name["window_end"]["parameterType"]["type"] == "DATE"
    assert by_name["window_end"]["parameterValue"]["value"] == "{{ ds }}"
    assert by_name["run_id"]["parameterValue"]["value"] == "{{ rh_run_id(ds, ts_nodash) }}"
    assert config["labels"]["dag"] == "ride_hailing_daily"


def test_mart_refresh_includes_all_four_views_in_dependency_order():
    sql = sql_jobs.query_configuration(sql_jobs.JOBS["refresh_mart_views"], SETTINGS)["query"]["query"]
    names = re.findall(r"marts/(\w+)\.sql", sql)
    assert names == ["v_daily_revenue", "v_zone_demand_hourly", "v_driver_leaderboard", "v_zone_demand_forecast"]
    assert "queryParameters" not in sql_jobs.query_configuration(sql_jobs.JOBS["refresh_mart_views"], SETTINGS)["query"]


def test_forecast_parameters_follow_the_bqml_sql_ranges():
    t = sql_jobs.param_templates(SETTINGS)
    assert 14 <= int(t["train_days"]) <= 365 and 1 <= int(t["top_n"]) <= 10 and 0 < float(t["confidence_level"]) < 1


# ---- Spark ----
def render(arg):
    values = {"ds": "2026-10-14", "rh_window_start(ds, params)": "2026-10-11", "rh_run_id(ds, ts_nodash)": "2026-10-14-20261015t010000"}
    return re.sub(r"\{\{\s*(.*?)\s*\}\}", lambda m: values[m.group(1)], arg)


def test_spark_arguments_are_accepted_by_the_spark_jobs_own_parser():
    sys.path.insert(0, str(ROOT / "spark"))
    try:
        from ridespark.config import parse_args
    finally:
        sys.path.remove(str(ROOT / "spark"))
    cfg = parse_args([render(a) for a in spark_batch.batch_args(SETTINGS)])
    assert str(cfg.processing_date) == "2026-10-14" and str(cfg.pickup_start) == "2026-10-11" and str(cfg.pickup_end) == "2026-10-14"
    assert cfg.output_path == "gs://rh-curated-test/curated" and cfg.timezone == "UTC"
    assert cfg.stream_input == "bq://labs-507309.raw_ds.rides_stream" and cfg.hist_input == "bq://labs-507309.raw_ds.rides_history"
    assert cfg.zones_input.endswith("taxi_zone_lookup.csv") and cfg.driver_snapshot and str(cfg.snapshot_date) == "2026-10-14"
    assert cfg.allow_missing_dimensions is True and cfg.run_id == "2026-10-14-20261015t010000"


def test_spark_arguments_without_optional_pieces():
    sys.path.insert(0, str(ROOT / "spark"))
    try:
        from ridespark.config import parse_args
    finally:
        sys.path.remove(str(ROOT / "spark"))
    plain = load_settings(ENV)
    args = [render(a) for a in spark_batch.batch_args(plain)]
    assert not any(a.startswith("--driver-snapshot") for a in args) and "--allow-missing-dimensions" not in args
    assert parse_args(args).driver_snapshot is None


def test_batch_definition_matches_the_dataproc_serverless_contract():
    batch = spark_batch.batch(SETTINGS)
    assert batch["pyspark_batch"]["main_python_file_uri"] == "gs://rh-artifacts-test/spark/code/run_job.py"
    assert batch["pyspark_batch"]["python_file_uris"] == ["gs://rh-artifacts-test/spark/code/ridespark.zip"]
    assert batch["environment_config"]["execution_config"]["service_account"] == SETTINGS.spark_service_account
    assert batch["runtime_config"]["properties"]["spark.sql.sources.partitionOverwriteMode"] == "dynamic"
    assert "subnetwork_uri" not in batch["environment_config"]["execution_config"]
    with_subnet = load_settings({**ENV, "RH_SPARK_SUBNET": "projects/p/regions/r/subnetworks/s"})
    assert spark_batch.batch(with_subnet)["environment_config"]["execution_config"]["subnetwork_uri"].endswith("/s")


def test_batch_id_is_unique_per_run_and_retry_and_valid_after_rendering():
    template = spark_batch.batch_id_template()
    rendered = template.replace("{{ ts_nodash | lower }}", "20261015t010000").replace("{{ ti.try_number }}", "2")
    assert re.fullmatch(r"[a-z][-a-z0-9]{2,60}[a-z0-9]", rendered)


def test_staging_layout_matches_the_spark_output_folders():
    from common.staging import SPECS

    spark_io = (ROOT / "spark" / "ridespark" / "io.py").read_text(encoding="utf-8")
    for spec in SPECS.values():
        assert f'"{spec.folder}"' in spark_io, spec.folder
        assert f'partitionBy("{spec.partition_key}")' in spark_io, spec.partition_key


def test_staging_tables_exist_in_the_ddl():
    ddl = (SQL / "ddl" / "30_staging_tables.sql").read_text(encoding="utf-8")
    from common.staging import SPECS

    for spec in SPECS.values():
        assert f"CREATE TABLE IF NOT EXISTS {spec.table}" in ddl


# ---- historical load ----
def test_historical_columns_match_the_ingestion_schema():
    sys.path.insert(0, str(ROOT))
    try:
        from ingestion.schemas import HIST_CSV_COLUMNS
    finally:
        sys.path.remove(str(ROOT))
    assert list(historical_sql.HIST_COLUMNS) == list(HIST_CSV_COLUMNS)


def test_historical_merge_script_is_rerunnable_and_guards_the_year():
    script = historical_sql.merge_script()
    assert script.index("BEGIN TRANSACTION") < script.index("DELETE FROM raw_ds.rides_history") \
        < script.index("INSERT INTO raw_ds.rides_history") < script.index("COMMIT TRANSACTION")
    assert "= 2018" in script and "no other year is substituted" in script
    assert "source_file = @source_file" in script and "raw_ds.rides_history_stg" in script
    params = historical_sql.merge_configuration(SETTINGS)["query"]["queryParameters"]
    assert {p["name"] for p in params} == set(dq.params_in_sql(script)) == set(historical_sql.PARAMS)
    assert params[0]["parameterValue"]["value"] == "gs://rh-landing-test/{{ rh_historical_object(ds, params) }}"


def test_historical_schema_fields_cover_every_csv_column():
    assert [f["name"] for f in historical_sql.schema_fields()] == [n for n, _ in historical_sql.HIST_COLUMNS]


def test_historical_target_columns_match_the_raw_ddl():
    ddl = (SQL / "ddl" / "11_raw_rides_history.sql").read_text(encoding="utf-8")
    for name, _ in historical_sql.HIST_COLUMNS:
        assert re.search(rf"^\s+{name} ", ddl, flags=re.M), name
    assert "source_file STRING NOT NULL" in ddl and "load_id STRING NOT NULL" in ddl


def test_documented_files_exist():
    for name in ("README.md", ".airflowignore", "config/environment.example.env", "config/variables.example.json",
                 "config/secret_manager.example.md"):
        assert (DAGS / name).is_file(), name
