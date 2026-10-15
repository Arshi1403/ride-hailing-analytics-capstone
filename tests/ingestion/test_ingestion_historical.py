import pytest

from ingestion.config import IngestionConfig
from ingestion.historical import (
    CSV_COLUMN_NAMES, LoadError, build_plan, check_header, execute, main, validate_gcs_uri,
)

CFG = IngestionConfig()
URI = "gs://bucket/nyc/yellow_tripdata_2018-01.csv"
HEADER = ",".join(CSV_COLUMN_NAMES)


class FakeGateway:
    def __init__(self, exists=True, header=HEADER, staged=100, in_year=90, fail_on=None):
        self.exists, self.header, self.staged, self.in_year, self.fail_on = exists, header, staged, in_year, fail_on
        self.calls = []

    def table_exists(self, table_fq):
        self.calls.append(("exists", table_fq))
        return self.exists

    def read_header(self, gcs_uri):
        self.calls.append(("header", gcs_uri))
        return self.header

    def load_csv(self, gcs_uri, table_fq, max_bad_records):
        self.calls.append(("load", table_fq, max_bad_records))
        if self.fail_on == "load":
            raise RuntimeError("load failed")
        return self.staged

    def query(self, sql, params):
        self.calls.append(("query", sql.split("\n")[0][:30], params))
        if "staged_rows" in sql:
            return [{"staged_rows": self.staged, "rows_in_year": self.in_year}]
        if sql.startswith("BEGIN TRANSACTION") and self.fail_on == "replace":
            raise RuntimeError("replace failed")
        if "COUNT(*) AS n" in sql:
            return [{"n": self.in_year}]
        return []

    def delete_table(self, table_fq):
        self.calls.append(("delete", table_fq))


def kinds(gateway):
    return [c[0] for c in gateway.calls]


@pytest.mark.parametrize("uri", [
    "bucket/file_2018.csv", "gs://bucket/", "gs://bucket/dir/", "gs://bucket/yellow_tripdata_2018-*.csv",
    "gs://bucket/yellow_tripdata_2019-01.csv", "gs://bucket/yellow_tripdata_2017-12.csv",
    "gs://bucket/2018/data.parquet",
])
def test_bad_uris_rejected(uri):
    with pytest.raises(LoadError):
        validate_gcs_uri(uri, CFG.hist_file_pattern)


@pytest.mark.parametrize("uri", [URI, "gs://bkt/x/trips_2018_sample.csv", "gs://bkt/yellow_tripdata_2018-12.csv.gz"])
def test_2018_uris_accepted(uri):
    validate_gcs_uri(uri, CFG.hist_file_pattern)


def test_other_year_error_says_no_substitution():
    with pytest.raises(LoadError, match="no other year"):
        validate_gcs_uri("gs://bkt/yellow_tripdata_2019-01.csv", CFG.hist_file_pattern)


def test_header_check():
    check_header(HEADER)
    check_header(HEADER.upper() + "\r\n")
    with pytest.raises(LoadError, match="header"):
        check_header("vendor_id,pickup_datetime")


def test_plan_sql_is_parameterised_and_rerun_safe():
    plan = build_plan(CFG, URI, sample_percent=10)
    assert plan.target_table == "labs-507309.raw_ds.rides_history"
    assert plan.staging_table.startswith("labs-507309.raw_ds.rides_history_stg_")
    sql = plan.replace_sql
    assert sql.index("DELETE FROM") < sql.index("INSERT INTO")
    assert sql.startswith("BEGIN TRANSACTION") and sql.rstrip().endswith("COMMIT TRANSACTION;")
    assert "@source_file" in sql and "@year" in sql and "@sample_percent" in sql
    assert URI not in sql  # values travel as query parameters, never in the SQL text


def test_plans_get_unique_load_ids():
    assert build_plan(CFG, URI).load_id != build_plan(CFG, URI).load_id


@pytest.mark.parametrize("sample,bad", [(0, 0), (101, 0), (50, -1)])
def test_plan_validates_numbers(sample, bad):
    with pytest.raises(LoadError):
        build_plan(CFG, URI, sample_percent=sample, max_bad_records=bad)


def test_execute_happy_path_order_and_cleanup():
    gateway = FakeGateway()
    result = execute(build_plan(CFG, URI), gateway)
    assert kinds(gateway) == ["exists", "header", "load", "query", "query", "query", "delete"]
    assert (result.staged_rows, result.rows_in_year, result.rows_outside_year, result.rows_loaded) == (100, 90, 10, 90)
    replace_call = gateway.calls[4]
    assert replace_call[2]["source_file"] == URI and replace_call[2]["year"] == 2018


def test_execute_rerun_gives_same_result():
    first = execute(build_plan(CFG, URI), FakeGateway())
    second = execute(build_plan(CFG, URI), FakeGateway())
    assert first.rows_loaded == second.rows_loaded


def test_missing_target_table_stops_before_any_load():
    gateway = FakeGateway(exists=False)
    with pytest.raises(LoadError, match="ingestion.schemas hist"):
        execute(build_plan(CFG, URI), gateway)
    assert kinds(gateway) == ["exists"]


def test_wrong_header_stops_before_load():
    gateway = FakeGateway(header="a,b,c")
    with pytest.raises(LoadError):
        execute(build_plan(CFG, URI), gateway)
    assert "load" not in kinds(gateway)


def test_file_without_2018_rows_writes_nothing_and_cleans_up():
    gateway = FakeGateway(in_year=0)
    with pytest.raises(LoadError, match="no rows with a 2018 pickup"):
        execute(build_plan(CFG, URI), gateway)
    assert gateway.calls[-1][0] == "delete"
    assert not any(c[0] == "query" and c[1].startswith("BEGIN") for c in gateway.calls)


def test_staging_cleaned_up_when_replace_fails():
    gateway = FakeGateway(fail_on="replace")
    with pytest.raises(RuntimeError):
        execute(build_plan(CFG, URI), gateway)
    assert gateway.calls[-1][0] == "delete"


def test_no_cleanup_when_load_itself_failed():
    gateway = FakeGateway(fail_on="load")
    with pytest.raises(RuntimeError):
        execute(build_plan(CFG, URI), gateway)
    assert "delete" not in kinds(gateway)


def test_cli_dry_run_makes_no_cloud_calls(monkeypatch, capsys):
    def boom(*args, **kwargs):
        raise AssertionError("cloud access attempted in dry run")
    monkeypatch.setattr("ingestion.historical.BigQueryGateway", boom)
    assert main(["--gcs-uri", URI, "--sample-percent", "5", "--print-ddl"]) == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out and "BEGIN TRANSACTION" in out and "CREATE TABLE IF NOT EXISTS" in out


def test_cli_rejects_wrong_year(capsys):
    assert main(["--gcs-uri", "gs://bkt/yellow_tripdata_2019-01.csv"]) == 2
    assert "2018" in capsys.readouterr().err
