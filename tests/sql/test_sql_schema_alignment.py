"""DDL and merge column lists must agree with the Spark and ingestion schemas (the contracts between agents)."""
import re
import sys

import pytest

from sql_helpers import DQ_COLUMNS, ROOT, ddl_columns, ddl_tail, read

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "spark"))

SPARK = pytest.importorskip("ridespark.schemas")  # importing pyspark types needs no JVM


def spark_columns(schema):
    return [(c["name"], c["type"]) for c in SPARK.bigquery_schema(schema)]


def test_curated_fact_matches_spark_output():
    assert ddl_columns(read("ddl", "20_curated_fact_rides"), "curated_ds.fact_rides") == spark_columns(SPARK.CURATED_RIDES_SCHEMA)


def test_rejected_rides_match_spark_output():
    assert ddl_columns(read("ddl", "22_curated_rejected_rides"), "curated_ds.rejected_rides") == spark_columns(SPARK.REJECTED_SCHEMA)


def test_staging_tables_match_spark_parquet_columns():
    text = read("ddl", "30_staging_tables")
    assert ddl_columns(text, "stg_ds.fact_rides_stg") == spark_columns(SPARK.CURATED_RIDES_SCHEMA)
    assert ddl_columns(text, "stg_ds.rejected_rides_stg") == spark_columns(SPARK.REJECTED_SCHEMA)
    assert ddl_columns(text, "stg_ds.dim_driver_scd2_stg") == spark_columns(SPARK.DRIVER_SCD2_SCHEMA) + [("snapshot_date", "DATE")]


def test_raw_tables_match_ingestion_schemas():
    ingestion = pytest.importorskip("ingestion.schemas")
    stream = [(n, t) for n, t, _ in ingestion.STREAM_COLUMNS]
    assert ddl_columns(read("ddl", "10_raw_rides_stream"), "raw_ds.rides_stream") == stream
    hist = list(ingestion.HIST_CSV_COLUMNS) + [(n, t) for n, t, _ in ingestion.HIST_META_COLUMNS]
    assert ddl_columns(read("ddl", "11_raw_rides_history"), "raw_ds.rides_history") == hist


def test_fact_is_partitioned_by_pickup_date_and_clustered_by_zone_and_driver():
    tail = ddl_tail(read("ddl", "20_curated_fact_rides"), "curated_ds.fact_rides")
    assert re.search(r"PARTITION BY pickup_date\b", tail)
    assert re.search(r"CLUSTER BY pickup_zone, driver_id\b", tail)


def test_rejected_rides_partitioned_by_event_date():
    assert "PARTITION BY event_date" in ddl_tail(read("ddl", "22_curated_rejected_rides"), "curated_ds.rejected_rides")


def test_scd2_dimension_has_timestamps_and_current_flag():
    cols = dict(ddl_columns(read("ddl", "21_curated_dim_driver_scd2"), "curated_ds.dim_driver_scd2"))
    assert cols["effective_start_ts"] == "TIMESTAMP" and cols["effective_end_ts"] == "TIMESTAMP"
    assert cols["is_current"] == "BOOL" and cols["driver_id"] == "STRING"


def test_spark_scd2_columns_are_all_carried_into_the_dimension():
    dim = {name for name, _ in ddl_columns(read("ddl", "21_curated_dim_driver_scd2"), "curated_ds.dim_driver_scd2")}
    for name, _ in spark_columns(SPARK.DRIVER_SCD2_SCHEMA):
        if name not in ("effective_from", "effective_to"):
            assert name in dim, name


def test_ops_dq_results_columns_equal_the_query_output_columns():
    cols = [n for n, _ in ddl_columns(read("ddl", "40_ml_and_ops_tables"), "ops_ds.dq_results")]
    assert cols == DQ_COLUMNS


def _lists(text):
    insert = re.search(r"INSERT \((.*?)\) VALUES \((.*?)\)\s*WHEN", text, flags=re.S)
    return ([c.strip() for c in insert.group(1).split(",")],
            [v.strip() for v in insert.group(2).split(",")])


def test_fact_merge_column_lists_match_the_table():
    text = read("merge", "merge_fact_rides")
    ddl = [n for n, _ in ddl_columns(read("ddl", "20_curated_fact_rides"), "curated_ds.fact_rides")]
    columns, values = _lists(text)
    assert columns == ddl
    assert values == [f"S.{c}" for c in ddl]
    updated = re.findall(r"^\s{2}(\w+) = S\.(\w+),?$", text, flags=re.M)
    assert all(a == b for a, b in updated)
    assert [a for a, _ in updated] == [c for c in ddl if c not in ("ride_id", "pickup_date")]


def test_rejected_merge_inserts_every_column():
    text = read("merge", "merge_rejected_rides")
    ddl = [n for n, _ in ddl_columns(read("ddl", "22_curated_rejected_rides"), "curated_ds.rejected_rides")]
    insert = re.search(r"INSERT INTO curated_ds\.rejected_rides \((.*?)\)\s*SELECT(.*?)FROM", text, flags=re.S)
    assert [c.strip() for c in insert.group(1).split(",")] == ddl
    assert [c.strip() for c in insert.group(2).split(",")] == ddl


def test_dimension_merge_inserts_every_dimension_column():
    text = read("merge", "merge_dim_driver_scd2")
    ddl = [n for n, _ in ddl_columns(read("ddl", "21_curated_dim_driver_scd2"), "curated_ds.dim_driver_scd2")]
    insert = re.search(r"INSERT \((.*?)\) VALUES", text, flags=re.S)
    assert [c.strip() for c in insert.group(1).split(",")] == ddl
