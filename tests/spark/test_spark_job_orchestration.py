"""Job orchestration with file I/O replaced by in-memory fakes, so these run on any machine (no Hadoop needed)."""
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from ridespark import job
from ridespark.scd2 import apply_snapshot, empty_history
from ridespark.schemas import CURATED_RIDES_SCHEMA, signature
from spark_helpers import D1, D2, ZONES, hist_df, hist_row, settle, snapshot_df, stream_df, stream_row, utc, zones_df

ARGS = ["--processing-date", "2026-10-15", "--stream-input", "s", "--hist-input", "h", "--zones-input", "z",
        "--driver-snapshot", "d", "--snapshot-date", "2026-10-01", "--output-path", "out", "--run-id", "t1"]


class FakeIO:
    def __init__(self):
        self.sources, self.stored, self.writes = {}, None, {}


@pytest.fixture
def io(spark, monkeypatch):
    fake = FakeIO()
    fake.sources = {
        "s": stream_df(spark, stream_row(ride_id="a"), stream_row(ride_id="bad", fare=Decimal("-5"))),
        "h": hist_df(spark, hist_row(tpep_pickup_datetime=utc(2026, 10, 13, 8), tpep_dropoff_datetime=utc(2026, 10, 13, 8, 20))),
        "z": zones_df(spark, *ZONES),
        "d": snapshot_df(spark, ("D1", "SEDAN", "1", "ACTIVE")),
    }

    def read_source(_spark, spec):
        if spec not in fake.sources:
            raise FileNotFoundError(spec)
        return fake.sources[spec]

    monkeypatch.setattr(job, "read_source", read_source)
    monkeypatch.setattr(job, "read_latest_scd2", lambda _spark, _base: fake.stored)
    monkeypatch.setattr(job, "write_rides", lambda df, base: fake.writes.update(rides=df.collect()))
    monkeypatch.setattr(job, "write_rejected", lambda df, base: fake.writes.update(rejected=df.collect()))
    monkeypatch.setattr(job, "write_scd2", lambda df, base, d: fake.writes.update(scd2=(df.collect(), d)))
    monkeypatch.setattr(job, "write_snapshot_conflicts", lambda df, base, d: fake.writes.update(conflicts=df.collect()))
    monkeypatch.setattr(job, "write_metrics", lambda _s, base, pd, run_id, m: fake.writes.update(metrics=m))
    return fake


def test_happy_path_writes_curated_rejected_scd2_and_metrics(spark, io):
    assert job.main(ARGS, spark=spark) == 0
    rides = {r.ride_id: r for r in io.writes["rides"]}
    assert len(rides) == 2 and "a" in rides
    assert rides["a"].zone_match == "MATCHED" and rides["a"].driver_match == "MATCHED"
    assert [(r.ride_id, r.reject_reason) for r in io.writes["rejected"]] == [("bad", "NEGATIVE_FARE")]
    rows, snapshot_date = io.writes["scd2"]
    assert snapshot_date == date(2026, 10, 1) and [(r.driver_id, r.is_current) for r in rows] == [("D1", True)]
    assert io.writes["metrics"]["curated_rows"] == 2 and io.writes["metrics"]["run_id"] == "t1"
    assert io.writes["metrics"]["pickup_start"] == date(2026, 10, 12)


def test_missing_zone_dimension_fails_before_any_write(spark, io):
    del io.sources["z"]
    assert job.main(ARGS, spark=spark) == 3
    assert io.writes == {}


def test_empty_zone_dimension_counts_as_unavailable(spark, io):
    io.sources["z"] = zones_df(spark)
    assert job.main(ARGS, spark=spark) == 3 and io.writes == {}


def test_missing_dimensions_can_be_allowed_and_are_labelled(spark, io):
    del io.sources["z"]
    del io.sources["d"]
    assert job.main(ARGS + ["--allow-missing-dimensions"], spark=spark) == 0
    rows = io.writes["rides"]
    assert {r.zone_match for r in rows} == {"DIMENSION_UNAVAILABLE"}
    assert {r.driver_match for r in rows} == {"DIMENSION_UNAVAILABLE"}
    assert {r.pickup_zone_name for r in rows} == {None} and {r.driver_vehicle_type for r in rows} == {None}
    assert "scd2" not in io.writes


def test_stored_history_is_used_when_no_snapshot_is_given(spark, io):
    io.stored = settle(spark, apply_snapshot(empty_history(spark), snapshot_df(spark, ("D1", "SUV", "1", "ACTIVE")), D1))
    args = [a for a in ARGS if a not in ("--driver-snapshot", "d", "--snapshot-date", "2026-10-01")]
    assert job.main(args, spark=spark) == 0
    assert {r.driver_vehicle_type for r in io.writes["rides"] if r.driver_id} == {"SUV"}
    assert "scd2" not in io.writes


def test_no_snapshot_and_no_history_is_a_missing_dimension(spark, io):
    args = [a for a in ARGS if a not in ("--driver-snapshot", "d", "--snapshot-date", "2026-10-01")]
    assert job.main(args, spark=spark) == 3 and io.writes == {}


def test_unreadable_snapshot_falls_back_only_when_allowed(spark, io):
    del io.sources["d"]
    assert job.main(ARGS, spark=spark) == 3
    assert job.main(ARGS + ["--allow-missing-dimensions"], spark=spark) == 0


def test_schema_mismatch_in_raw_input_exits_4_and_writes_nothing(spark, io):
    io.sources["s"] = io.sources["s"].withColumn("pickup_ts", F.col("pickup_ts").cast("string"))
    assert job.main(ARGS, spark=spark) == 4 and io.writes == {}


def test_schema_mismatch_in_dimensions_exits_4(spark, io):
    io.sources["d"] = io.sources["d"].drop("status")
    assert job.main(ARGS, spark=spark) == 4
    io.sources["d"] = snapshot_df(spark, ("D1", "SEDAN", "1", "ACTIVE"))
    io.sources["z"] = zones_df(spark, *ZONES, ("1", "Dup", "X", "Y"))
    assert job.main(ARGS, spark=spark) == 4 and io.writes == {}


def test_snapshot_older_than_stored_history_exits_5(spark, io):
    io.stored = settle(spark, apply_snapshot(empty_history(spark), snapshot_df(spark, ("D1", "SUV", "1", "ACTIVE")), D2))
    assert job.main(ARGS, spark=spark) == 5 and io.writes == {}


def test_repeating_a_snapshot_date_does_not_change_history(spark, io):
    assert job.main(ARGS, spark=spark) == 0
    first_rows = io.writes["scd2"][0]
    io.stored = settle(spark, apply_snapshot(empty_history(spark), io.sources["d"], D1))
    assert job.main(ARGS, spark=spark) == 0
    assert sorted(map(tuple, io.writes["scd2"][0])) == sorted(map(tuple, first_rows))


def test_conflicting_snapshot_rows_are_reported_not_applied(spark, io):
    io.sources["d"] = snapshot_df(spark, ("D1", "SEDAN", "1", "ACTIVE"), ("D1", "SUV", "1", "ACTIVE"), ("D2", "MINI", "2", "ACTIVE"))
    assert job.main(ARGS, spark=spark) == 0
    assert {r.driver_id for r in io.writes["scd2"][0]} == {"D2"}
    assert len(io.writes["conflicts"]) == 2 and io.writes["metrics"]["driver_snapshot_conflicts"] == 2


def test_curated_output_matches_published_schema(spark, io, monkeypatch):
    captured = {}
    monkeypatch.setattr(job, "write_rides", lambda df, base: captured.update(schema=signature(df)))
    assert job.main(ARGS, spark=spark) == 0
    assert captured["schema"] == signature(CURATED_RIDES_SCHEMA)


@pytest.mark.parametrize("argv", [
    [], ["--processing-date", "15/10/2026", "--output-path", "o", "--stream-input", "s"],
    ["--processing-date", "2026-10-15", "--output-path", "o"],
])
def test_bad_arguments_exit_2_and_touch_nothing(spark, io, argv, capsys):
    assert job.main(argv, spark=spark) == 2 and io.writes == {}
