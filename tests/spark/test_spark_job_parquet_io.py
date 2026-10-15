"""End-to-end runs against real Parquet/CSV files in a temp folder (still no cloud).
Skipped automatically when the machine cannot do local Parquet I/O (Windows without winutils/hadoop.dll)."""
import csv
import json
from decimal import Decimal

from pyspark.sql import functions as F

from ridespark.job import main
from ridespark.schemas import CURATED_RIDES_SCHEMA, signature
from spark_helpers import hist_df, hist_row, snapshot_df, stream_df, stream_row, utc

import pytest

pytestmark = pytest.mark.usefixtures("local_parquet_io")


def write_inputs(spark, tmp_path, stream=None):
    stream = stream if stream is not None else stream_df(spark, stream_row(ride_id="a"))
    hist = hist_df(spark, hist_row(tpep_pickup_datetime=utc(2026, 10, 13, 8), tpep_dropoff_datetime=utc(2026, 10, 13, 8, 20)))
    paths = {"stream": tmp_path / "raw_stream", "hist": tmp_path / "raw_hist", "drivers": tmp_path / "drivers0"}
    stream.write.mode("overwrite").parquet(str(paths["stream"]))
    hist.write.mode("overwrite").parquet(str(paths["hist"]))
    snapshot_df(spark, ("D1", "SEDAN", "1", "ACTIVE")).write.mode("overwrite").parquet(str(paths["drivers"]))
    zones = tmp_path / "zones.csv"
    with zones.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerows([["LocationID", "Borough", "Zone", "service_zone"], ["1", "Manhattan", "Alpha", "Yellow Zone"],
                          ["2", "Queens", "Beta", "Boro Zone"]])
    return paths, zones


def argv(paths, zones, out, *extra):
    return ["--processing-date", "2026-10-15", "--stream-input", str(paths["stream"]), "--hist-input", str(paths["hist"]),
            "--zones-input", str(zones), "--output-path", str(out), "--run-id", "test-run",
            "--driver-snapshot", str(paths["drivers"]), "--snapshot-date", "2026-10-01", *extra]


def read(spark, out, name):
    return spark.read.parquet(str(out / name))


def content(df):
    return sorted(tuple(str(v) for v in row) for row in df.collect())


def test_job_writes_partitioned_parquet_rejects_and_metrics(spark, tmp_path):
    paths, zones = write_inputs(spark, tmp_path, stream=stream_df(
        spark, stream_row(ride_id="a"), stream_row(ride_id="bad", fare=Decimal("-5"))))
    out = tmp_path / "out"
    assert main(argv(paths, zones, out), spark=spark) == 0
    assert (out / "rides" / "pickup_date=2026-10-14").is_dir() and (out / "rides" / "pickup_date=2026-10-13").is_dir()
    rides = read(spark, out, "rides")
    assert rides.count() == 2 and signature(rides)[:-1] == signature(CURATED_RIDES_SCHEMA)[:-1]
    assert [(r.ride_id, r.reject_reason) for r in read(spark, out, "rejected").collect()] == [("bad", "NEGATIVE_FARE")]
    metrics_file = next((out / "_run_metrics").rglob("part-*"))
    assert json.loads(metrics_file.read_text())["curated_rows"] == 2


def test_rerunning_the_same_window_is_safe(spark, tmp_path):
    paths, zones = write_inputs(spark, tmp_path)
    out = tmp_path / "out"
    assert main(argv(paths, zones, out), spark=spark) == 0
    first = content(read(spark, out, "rides"))
    assert main(argv(paths, zones, out), spark=spark) == 0
    assert main(argv(paths, zones, out), spark=spark) == 0
    assert content(read(spark, out, "rides")) == first
    assert read(spark, out, "dim_driver_scd2").count() == 1


def test_rerun_replaces_only_the_partitions_in_its_window(spark, tmp_path):
    paths, zones = write_inputs(spark, tmp_path)
    out = tmp_path / "out"
    assert main(argv(paths, zones, out), spark=spark) == 0
    write_inputs(spark, tmp_path, stream=stream_df(
        spark, stream_row(ride_id="a", fare=Decimal("99.00"), ingest_ts=utc(2026, 10, 14, 12))))
    assert main(argv(paths, zones, out, "--pickup-start-date", "2026-10-14", "--pickup-end-date", "2026-10-14"), spark=spark) == 0
    rides = read(spark, out, "rides")
    assert rides.where(F.col("ride_id") == "a").first().fare == Decimal("99")
    assert rides.where(F.col("pickup_date") == "2026-10-13").count() == 1


def test_driver_snapshots_are_stored_and_reused(spark, tmp_path):
    paths, zones = write_inputs(spark, tmp_path)
    out = tmp_path / "out"
    day2 = tmp_path / "snap2"
    snapshot_df(spark, ("D1", "SUV", "1", "ACTIVE")).write.parquet(str(day2))
    assert main(argv(paths, zones, out), spark=spark) == 0
    assert main(argv(paths, zones, out, "--driver-snapshot", str(day2), "--snapshot-date", "2026-10-15"), spark=spark) == 0
    history = spark.read.parquet(str(out / "dim_driver_scd2")).where("snapshot_date = '2026-10-15'")
    assert sorted((r.vehicle_type, r.is_current) for r in history.collect()) == [("SEDAN", False), ("SUV", True)]
    assert read(spark, out, "rides").where(F.col("ride_id") == "a").first().driver_vehicle_type == "SEDAN"
    assert main(argv(paths, zones, out), spark=spark) == 5  # older snapshot date is refused


def test_missing_zone_file_fails_without_output(spark, tmp_path):
    paths, zones = write_inputs(spark, tmp_path)
    out = tmp_path / "out"
    assert main(argv(paths, tmp_path / "missing.csv", out), spark=spark) == 3
    assert not (out / "rides").exists()
