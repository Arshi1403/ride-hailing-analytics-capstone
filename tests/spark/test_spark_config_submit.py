import pytest

from ridespark.config import JobConfigError, parse_args
from ridespark.submit import SubmitConfigError, build_command

BASE = ["--processing-date", "2026-10-15", "--stream-input", "/data/s", "--output-path", "/out"]


def test_defaults_window_and_run_id():
    cfg = parse_args(BASE)
    assert (str(cfg.pickup_start), str(cfg.pickup_end), cfg.timezone) == ("2026-10-12", "2026-10-15", "UTC")
    assert cfg.run_id.startswith("2026-10-15-") and cfg.snapshot_date == cfg.processing_date
    assert not cfg.allow_missing_dimensions


@pytest.mark.parametrize("extra,fragment", [
    (["--processing-date", "x"], "YYYY-MM-DD"),
    (["--timezone", "Not A Zone!"], "timezone"),
    (["--run-id", "bad id"], "run-id"),
    (["--lookback-days", "99"], "lookback"),
    (["--pickup-start-date", "2026-10-20"], "start is after"),
    (["--pickup-start-date", "2020-01-01"], "longer than"),
    (["--snapshot-date", "2026-10-16"], "snapshot-date"),
    (["--zones-input", "bq://bad"], "bq://project.dataset.table"),
])
def test_invalid_arguments(extra, fragment):
    with pytest.raises(JobConfigError, match=fragment):
        parse_args(BASE + extra)


def test_needs_an_input():
    with pytest.raises(JobConfigError, match="at least one"):
        parse_args(["--processing-date", "2026-10-15", "--output-path", "/out"])


ENV = {
    "SPARK_MAIN_URI": "gs://bkt/spark/code/run_job.py", "SPARK_PY_FILES_URI": "gs://bkt/spark/code/ridespark.zip",
    "DATAPROC_DEPS_BUCKET": "gs://bkt", "SPARK_OUTPUT_PATH": "gs://bkt/curated",
    "DATAPROC_SERVICE_ACCOUNT": "sa-spark@labs-507309.iam.gserviceaccount.com",
    "DATAPROC_BATCH_ID": "rides-curation-20261015",
}


def test_submit_command_contains_required_pieces():
    command = " ".join(build_command(ENV, "2026-10-15"))
    for expected in ("gcloud dataproc batches submit pyspark gs://bkt/spark/code/run_job.py", "--region=asia-south1",
                     "--project=labs-507309", "--version=2.2", "--batch=rides-curation-20261015",
                     "--deps-bucket=gs://bkt", "--py-files=gs://bkt/spark/code/ridespark.zip",
                     "--processing-date=2026-10-15", "--output-path=gs://bkt/curated",
                     "--stream-input=bq://labs-507309.raw_ds.rides_stream",
                     "--hist-input=bq://labs-507309.raw_ds.rides_history"):
        assert expected in command
    assert command.index(" -- ") < command.index("--processing-date")


def test_submit_reports_every_missing_setting():
    with pytest.raises(SubmitConfigError) as info:
        build_command({}, "2026-10-15")
    assert len(info.value.problems) == 5


def test_submit_optional_inputs_and_subnet():
    env = {**ENV, "DATAPROC_SUBNET": "default", "SPARK_ZONES_INPUT": "gs://bkt/z.csv", "SPARK_TIMEZONE": "Asia/Kolkata"}
    command = build_command(env, "2026-10-15", ["--allow-missing-dimensions"])
    assert "--subnet=default" in command and "--zones-input=gs://bkt/z.csv" in command
    assert "--timezone=Asia/Kolkata" in command and command[-1] == "--allow-missing-dimensions"


def test_submit_rejects_bad_date():
    with pytest.raises(SubmitConfigError, match="YYYY-MM-DD"):
        build_command(ENV, "tomorrow")
