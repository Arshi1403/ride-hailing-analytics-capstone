"""Branch logic, data-quality handling, staging, alerting and secrets hygiene. Pure Python: no Airflow, no network."""
import logging
import re
from datetime import datetime, timezone

import pytest

import dag_loader  # noqa: F401
from common import alerts, dq
from common.config import load_settings
from common.staging import SPECS, StagingError, StagingSkip, run_staging_load
from dag_loader import DAGS, ENV

SETTINGS = load_settings(ENV)


def row(check_id, status):
    return {"check_id": check_id, "status": status, "detail": "do not forward me", "observed_value": 1.0}


# ---- dq summary and branch ----
def test_summary_is_compact_and_contains_only_counts_and_ids():
    rows = [row("A", "PASS"), row("B", "WARN"), row("C", "FAIL"), row("D", "FAIL")]
    summary = dq.summarize(rows, "run-1")
    assert summary == {"run_id": "run-1", "checks": 4, "passed": 1, "warned": 1, "failed": 2,
                       "failed_ids": ["C", "D"], "warned_ids": ["B"]}
    assert "do not forward me" not in str(summary)


def test_summary_size_is_capped_for_xcom():
    rows = [row(f"C{i:03d}", "FAIL") for i in range(500)]
    summary = dq.summarize(rows, "r")
    assert len(summary["failed_ids"]) == dq.MAX_IDS_IN_SUMMARY and summary["failed"] == 500
    assert len(str(summary)) < 1500


@pytest.mark.parametrize("summary,expected", [
    ({"checks": 10, "failed": 0, "warned": 2}, dq.BRANCH_PASS),
    ({"checks": 10, "failed": 1, "warned": 0}, dq.BRANCH_FAIL),
    ({"checks": 0, "failed": 0}, dq.BRANCH_FAIL),  # nothing was checked
    (None, dq.BRANCH_FAIL),
    ({}, dq.BRANCH_FAIL),
    ("garbage", dq.BRANCH_FAIL),
])
def test_branch_on_dq_routes_failures_and_missing_results_to_the_alert_path(summary, expected):
    assert dq.decide_dq_branch(summary) == expected


def test_warnings_alone_do_not_block():
    assert dq.decide_dq_branch(dq.summarize([row("A", "PASS"), row("B", "WARN")], "r")) == dq.BRANCH_PASS


def test_failure_message_names_the_run_checks_and_where_to_look():
    text = dq.dq_failure_message({"run_id": "r1", "failed": 2, "warned": 1, "failed_ids": ["DQ-RC-01", "DQ-NF-01"]})
    assert "r1" in text and "DQ-RC-01" in text and "ops_ds.dq_results" in text and "not refreshed" in text


# ---- weekly retrain branch ----
MONDAY, TUESDAY = datetime(2026, 10, 12, tzinfo=timezone.utc), datetime(2026, 10, 13, tzinfo=timezone.utc)


def test_retrain_only_on_the_weekly_day():
    assert dq.decide_retrain(MONDAY, {}, SETTINGS, True)[0] == dq.BRANCH_RETRAIN
    assert dq.decide_retrain(TUESDAY, {}, SETTINGS, True)[0] == dq.BRANCH_SKIP_RETRAIN


def test_six_of_seven_days_do_not_retrain():
    from datetime import timedelta

    branches = [dq.decide_retrain(MONDAY + timedelta(days=i), {}, SETTINGS, True)[0] for i in range(7)]
    assert branches.count(dq.BRANCH_RETRAIN) == 1


def test_retrain_is_disabled_by_default_flag_even_on_the_weekly_day():
    branch, reason = dq.decide_retrain(MONDAY, {}, SETTINGS, enabled=False)
    assert branch == dq.BRANCH_SKIP_RETRAIN and "rh_enable_bqml_retrain" in reason
    assert dq.decide_retrain(MONDAY, {"force_retrain": True}, SETTINGS, enabled=False)[0] == dq.BRANCH_SKIP_RETRAIN


def test_backfill_runs_do_not_retrain_unless_forced():
    assert dq.decide_retrain(MONDAY, {"backfill_days": 7}, SETTINGS, True)[0] == dq.BRANCH_SKIP_RETRAIN
    assert dq.decide_retrain(TUESDAY, {"force_retrain": True}, SETTINGS, True)[0] == dq.BRANCH_RETRAIN
    assert dq.decide_retrain(TUESDAY, {"force_retrain": "false"}, SETTINGS, True)[0] == dq.BRANCH_SKIP_RETRAIN


def test_retrain_weekday_is_configurable():
    tuesday_settings = load_settings({**ENV, "RH_RETRAIN_WEEKDAY": "1"})
    assert dq.decide_retrain(TUESDAY, {}, tuesday_settings, True)[0] == dq.BRANCH_RETRAIN
    assert dq.decide_retrain(MONDAY, {}, tuesday_settings, True)[0] == dq.BRANCH_SKIP_RETRAIN


# ---- running dq SQL with an injected BigQuery ----
class FakeBq:
    def __init__(self, fail_on=None):
        self.queries, self.appended, self.fail_on = [], [], fail_on

    def run_query(self, sql, params, max_bytes):
        self.queries.append((sql, params, max_bytes))
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("quota exceeded token=abc123")
        return [row("X-" + str(len(self.queries)), "PASS")]

    def append_rows(self, table, rows):
        self.appended.append((table, rows))


VALUES = {"run_id": "r1", "window_start": "2026-10-11", "window_end": "2026-10-14", "business_tz": "UTC",
          "snapshot_date": "2026-10-14", "as_of_ts": "2026-10-15 00:00:00+00:00", "top_n": "10"}


def reader(relative):
    return (DAGS.parent / "sql" / relative).read_text(encoding="utf-8")


def test_run_checks_runs_every_dq_file_binds_typed_parameters_and_stores_results():
    bq = FakeBq()
    rows = dq.run_checks(bq, reader, VALUES, 123)
    assert len(bq.queries) == 6 and len(rows) == 6
    assert all(q[2] == 123 for q in bq.queries)
    first_params = {name: (kind, value) for name, kind, value in bq.queries[0][1]}
    assert first_params["window_start"][0] == "DATE" and str(first_params["window_start"][1]) == "2026-10-11"
    assert first_params["run_id"] == ("STRING", "r1")
    assert bq.appended and bq.appended[0][0] == "ops_ds.dq_results" and len(bq.appended[0][1]) == 6


def test_missing_parameter_is_a_clear_permanent_error():
    with pytest.raises(dq.DqRunError, match="needs parameter @business_tz"):
        dq.run_checks(FakeBq(), reader, {k: v for k, v in VALUES.items() if k != "business_tz"}, 1)


def test_query_failures_are_retriable_and_name_the_file():
    with pytest.raises(dq.DqQueryError, match="dq_negative_fares.sql failed"):
        dq.run_checks(FakeBq(fail_on="DQ-NF-01"), reader, VALUES, 1)


def test_forecast_validation_uses_its_own_parameters():
    bq = FakeBq()
    dq.run_checks(bq, reader, VALUES, 1, files=(dq.FORECAST_VALIDATION_FILE,))
    assert {p[0] for p in bq.queries[0][1]} == {"as_of_ts", "top_n", "run_id"}
    assert [type(p[2]).__name__ for p in bq.queries[0][1] if p[0] == "as_of_ts"] == ["datetime"]


# ---- staging ----
class FakeGcs:
    def __init__(self, prefixes):
        self.prefixes = set(prefixes)

    def prefix_has_objects(self, bucket, prefix):
        return (bucket, prefix) in self.prefixes


class FakeStagingBq:
    def __init__(self):
        self.loads, self.emptied = [], []

    def load_parquet(self, uris, table, hive_prefix):
        self.loads.append((uris, table, hive_prefix))
        return 42

    def empty_table(self, table):
        self.emptied.append(table)


def prefixes(folder, key, days):
    return [("rh-curated-test", f"curated/{folder}/{key}={d}/") for d in days]


def test_rides_staging_loads_only_window_partitions_that_exist():
    gcs = FakeGcs(prefixes("rides", "pickup_date", ["2026-10-12", "2026-10-14"]) + prefixes("rides", "pickup_date", ["2026-09-01"]))
    bq = FakeStagingBq()
    out = run_staging_load(SETTINGS, SPECS["load_staging_rides"], "2026-10-14", {}, gcs, bq)
    uris, table, hive = bq.loads[0]
    assert uris == ["gs://rh-curated-test/curated/rides/pickup_date=2026-10-12/*",
                    "gs://rh-curated-test/curated/rides/pickup_date=2026-10-14/*"]  # older Spark output is not loaded
    assert table == "stg_ds.fact_rides_stg" and hive == "gs://rh-curated-test/curated/rides/"
    assert out["partitions_loaded"] == 2 and out["partitions_missing"] == 2


def test_backfill_window_covers_seven_dates():
    gcs = FakeGcs(prefixes("rides", "pickup_date", [f"2026-10-{d:02d}" for d in range(8, 15)]))
    bq = FakeStagingBq()
    run_staging_load(SETTINGS, SPECS["load_staging_rides"], "2026-10-14", {"backfill_days": 7}, gcs, bq)
    assert len(bq.loads[0][0]) == 7


def test_missing_required_rides_output_is_a_fatal_error_with_a_hint():
    with pytest.raises(StagingError, match="run_curated_spark"):
        run_staging_load(SETTINGS, SPECS["load_staging_rides"], "2026-10-14", {}, FakeGcs([]), FakeStagingBq())


def test_no_rejects_empties_the_staging_table_instead_of_failing():
    bq = FakeStagingBq()
    out = run_staging_load(SETTINGS, SPECS["load_staging_rejected"], "2026-10-14", {}, FakeGcs([]), bq)
    assert bq.emptied == ["stg_ds.rejected_rides_stg"] and bq.loads == [] and out["rows"] == 0


def test_dimension_staging_is_skipped_without_a_driver_snapshot_and_loads_only_todays_snapshot():
    with pytest.raises(StagingSkip, match="RH_DRIVER_SNAPSHOT_URI"):
        run_staging_load(SETTINGS, SPECS["load_staging_dim"], "2026-10-14", {}, FakeGcs([]), FakeStagingBq())
    with_snapshot = load_settings({**ENV, "RH_DRIVER_SNAPSHOT_URI": "gs://b/d.parquet"})
    gcs = FakeGcs(prefixes("dim_driver_scd2", "snapshot_date", ["2026-10-14", "2026-10-13"]))
    bq = FakeStagingBq()
    run_staging_load(with_snapshot, SPECS["load_staging_dim"], "2026-10-14", {}, gcs, bq)
    assert bq.loads[0][0] == ["gs://rh-curated-test/curated/dim_driver_scd2/snapshot_date=2026-10-14/*"]
    with pytest.raises(StagingError):
        run_staging_load(with_snapshot, SPECS["load_staging_dim"], "2026-10-15", {}, gcs, FakeStagingBq())


def test_rerunning_the_staging_load_is_a_repeat_of_the_same_operation():
    gcs = FakeGcs(prefixes("rides", "pickup_date", ["2026-10-14"]))
    first, second = FakeStagingBq(), FakeStagingBq()
    run_staging_load(SETTINGS, SPECS["load_staging_rides"], "2026-10-14", {}, gcs, first)
    run_staging_load(SETTINGS, SPECS["load_staging_rides"], "2026-10-14", {}, gcs, second)
    assert first.loads == second.loads  # the load replaces the staging table (WRITE_TRUNCATE), so it is repeatable


# ---- alerts and secrets ----
@pytest.mark.parametrize("secret", [
    "password=hunter2", "token: abc.def.ghi", "Authorization: Bearer abcdefghijklmnop", "ya29.a0AfH6SMBxyz-123",
    "AIzaSyA1234567890abcdefghijklmn", "https://user:pass@example.com/x", "https://hooks.slack.com/services/T000/B000/XXXX",
    "-----BEGIN PRIVATE KEY-----\nMIIabc\n-----END PRIVATE KEY-----", "api_key=ZZZ999",
])
def test_redaction_removes_credentials(secret):
    cleaned = alerts.redact(f"failed with {secret} while loading")
    for needle in ("hunter2", "abc.def.ghi", "abcdefghijklmnop", "a0AfH6SMBxyz", "AIzaSyA", "user:pass", "T000/B000", "MIIabc", "ZZZ999"):
        assert needle not in cleaned


def test_failure_message_is_short_and_has_no_secrets():
    class TI:
        dag_id, task_id, try_number = "ride_hailing_daily", "merge_fact", 3
        log_url = "https://airflow.example/log?task_id=merge_fact"

    context = {"task_instance": TI(), "run_id": "scheduled__x", "exception": RuntimeError("boom password=s3cr3t " + "x" * 2000)}
    message = alerts.failure_message(context)
    assert "ride_hailing_daily.merge_fact" in message and "try=3" in message and "RuntimeError" in message
    assert "s3cr3t" not in message and len(message) <= alerts.MAX_MESSAGE


def test_send_alert_never_raises_and_logs_when_no_connection_exists(caplog):
    with caplog.at_level(logging.INFO):
        assert alerts.send_alert("failed token=abc", "missing_conn") is False
    assert "abc" not in caplog.text and "[rh][alert]" in caplog.text


def test_success_message_is_compact():
    message = alerts.success_message("ride_hailing_daily", "r1", {"checks": 40, "warned": 2, "failed": 0}, {"checks": 8, "failed": 0})
    assert message == "SUCCESS ride_hailing_daily run=r1 | dq checks=40 warn=2 fail=0 | forecast checks=8 fail=0"


def test_sla_callback_names_the_tasks():
    _, _, on_sla = alerts.make_callbacks("missing_conn")

    class Dag:
        dag_id = "ride_hailing_daily"

    class Sla:
        task_id = "merge_fact"

    on_sla(Dag(), "", "", [Sla()], [])  # must not raise


def test_no_credentials_or_secret_reads_in_dag_code():
    patterns = [r"-----BEGIN", r"AIza[0-9A-Za-z_-]{20,}", r"ya29\.", r"(?i)password\s*=\s*['\"][^'\"]+['\"]",
                r"(?i)secret_key", r"\.json['\"]"]
    for path in DAGS.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for pattern in patterns:
            if path.name == "alerts.py":
                break  # the redaction module has to contain the patterns it removes
            assert not re.search(pattern, text), f"{path.name}: {pattern}"
        assert "Variable.get(" not in text or path.name == "tasks.py"  # no parse-time Variable reads
        assert "get_connection" not in text or path.name == "alerts.py"  # only the alert sender reads a connection


def test_only_compact_dicts_are_returned_to_xcom():
    source = (DAGS / "common" / "tasks.py").read_text(encoding="utf-8")
    for value in re.findall(r"^\s+return (.+)$", source, flags=re.M):
        assert not re.search(r"(?i)conn|secret|token|password|environ", value), value
    assert "xcom_push" not in source
