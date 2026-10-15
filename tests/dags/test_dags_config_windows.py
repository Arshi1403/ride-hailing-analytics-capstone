import pytest

import dag_loader  # noqa: F401  (puts dags/ on sys.path)
from common import windows
from common.config import ConfigError, load_settings
from dag_loader import ENV


def test_defaults_come_from_the_project_conventions():
    s = load_settings(ENV)
    assert (s.project_id, s.region, s.bq_location) == ("labs-507309", "asia-south1", "asia-south1")
    assert s.schedule_cron == "0 1 * * *" and s.schedule_tz == "Asia/Kolkata" and s.business_tz == "UTC"
    assert s.output_path == "gs://rh-curated-test/curated"
    assert s.spark_main_uri == "gs://rh-artifacts-test/spark/code/run_job.py"
    assert s.spark_zip_uri == "gs://rh-artifacts-test/spark/code/ridespark.zip"
    assert s.stream_input == "bq://labs-507309.raw_ds.rides_stream"
    assert s.hist_input == "bq://labs-507309.raw_ds.rides_history"
    assert s.zones_uri == "gs://rh-landing-test/reference/taxi_zone_lookup.csv"
    assert s.lookback_days == 3 and s.retrain_weekday == 0 and s.top_n == 10
    assert s.max_bytes_billed == 10_000_000_000  # capstone guardrail: 10 GB per query
    assert s.historical_required is True and s.allow_missing_dimensions is False
    assert s.sql_dir == "/home/airflow/gcs/data/sql" and s.alert_conn_id == "rh_alert_webhook"


def test_missing_required_settings_are_all_listed_with_a_hint():
    with pytest.raises(ConfigError) as info:
        load_settings({})
    text = str(info.value)
    for name in ("RH_LANDING_BUCKET", "RH_CURATED_BUCKET", "RH_ARTIFACTS_BUCKET", "RH_SPARK_SERVICE_ACCOUNT"):
        assert name in text
    assert "bucket" in text and len(info.value.problems) == 4


@pytest.mark.parametrize("override,fragment", [
    ({"RH_LANDING_BUCKET": "gs://bucket"}, "RH_LANDING_BUCKET"),
    ({"RH_SPARK_SERVICE_ACCOUNT": "not-an-email"}, "RH_SPARK_SERVICE_ACCOUNT"),
    ({"GCP_PROJECT_ID": "Bad_Project"}, "GCP_PROJECT_ID"),
    ({"RH_SCHEDULE_CRON": "0 1 * *"}, "RH_SCHEDULE_CRON"),
    ({"RH_BUSINESS_TZ": "Not A Zone"}, "RH_BUSINESS_TZ"),
    ({"RH_LOOKBACK_DAYS": "99"}, "RH_LOOKBACK_DAYS"),
    ({"RH_LOOKBACK_DAYS": "x"}, "RH_LOOKBACK_DAYS"),
    ({"RH_FORECAST_TOP_N": "11"}, "RH_FORECAST_TOP_N"),
    ({"RH_RETRAIN_WEEKDAY": "7"}, "RH_RETRAIN_WEEKDAY"),
    ({"RH_FORECAST_CONFIDENCE": "1.5"}, "RH_FORECAST_CONFIDENCE"),
    ({"RH_ALERT_EMAILS": "a@b.com, nope"}, "RH_ALERT_EMAILS"),
    ({"RH_ZONES_URI": "/local/zones.csv"}, "RH_ZONES_URI"),
    ({"RH_DRIVER_SNAPSHOT_URI": "http://x"}, "RH_DRIVER_SNAPSHOT_URI"),
    ({"RH_MAX_BYTES_BILLED": "5"}, "RH_MAX_BYTES_BILLED"),
])
def test_invalid_settings_are_rejected(override, fragment):
    with pytest.raises(ConfigError, match=fragment):
        load_settings({**ENV, **override})


def test_optional_settings_are_parsed():
    s = load_settings({**ENV, "RH_ALERT_EMAILS": "a@b.com, c@d.org", "RH_HISTORICAL_DUMP_REQUIRED": "false",
                       "RH_ALLOW_MISSING_DIMENSIONS": "TRUE", "RH_DRIVER_SNAPSHOT_URI": "gs://b/d.parquet",
                       "RH_SPARK_SUBNET": "projects/p/regions/r/subnetworks/s"})
    assert s.alert_emails == ("a@b.com", "c@d.org") and s.historical_required is False
    assert s.allow_missing_dimensions is True and s.driver_snapshot_uri == "gs://b/d.parquet"


# ---- windows ----
def test_daily_window_uses_the_lookback():
    assert windows.window_start("2026-10-14", {}, 3) == "2026-10-11"
    assert windows.window_start("2026-10-14", {"backfill_days": 0}, 0) == "2026-10-14"
    assert windows.window_start("2026-10-14", None, 3) == "2026-10-11"


@pytest.mark.parametrize("days,start", [(1, "2026-10-14"), (7, "2026-10-08")])
def test_backfill_recomputes_exactly_the_last_n_dates(days, start):
    assert windows.window_start("2026-10-14", {"backfill_days": days}, 3) == start
    assert len(windows.window_dates(start, "2026-10-14")) == days


@pytest.mark.parametrize("bad", [-1, 8, "x", "3.5"])
def test_backfill_is_limited_to_seven_days(bad):
    with pytest.raises(ValueError, match="backfill_days"):
        windows.backfill_days({"backfill_days": bad})


def test_window_dates_are_inclusive_and_ordered():
    assert windows.window_dates("2026-10-12", "2026-10-14") == ["2026-10-12", "2026-10-13", "2026-10-14"]
    with pytest.raises(ValueError):
        windows.window_dates("2026-10-15", "2026-10-14")


def test_run_id_is_stable_and_spark_safe():
    value = windows.run_id("2026-10-14", "20261015T010000")
    assert value == "2026-10-14-20261015t010000"
    assert windows.run_id("2026-10-14", "20261015T010000") == value
    with pytest.raises(ValueError):
        windows.run_id("2026-10-14", "bad value:")


def test_forecast_origin_is_the_next_midnight_floored_to_the_hour():
    assert windows.as_of_ts("2026-10-14", "UTC") == "2026-10-15 00:00:00+00:00"
    try:
        value = windows.as_of_ts("2026-10-14", "Asia/Kolkata")
    except Exception:  # noqa: BLE001 - tz database missing on this machine
        pytest.skip("tz database not available")
    assert value == "2026-10-14 18:00:00+00:00"  # 00:00 IST = 18:30 UTC, floored


def test_historical_object_template_override_and_path_guard():
    template = "historical/{ds}/trips_2018.csv"
    assert windows.historical_object("2026-10-14", {}, template) == "historical/2026-10-14/trips_2018.csv"
    assert windows.historical_object("2026-10-14", {"historical_object": "x/yellow_2018.csv"}, template) == "x/yellow_2018.csv"
    for bad in ("gs://other/x.csv", "/abs/x.csv", "a/../b.csv"):
        with pytest.raises(ValueError):
            windows.historical_object("2026-10-14", {"historical_object": bad}, template)
