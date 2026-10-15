"""Structural checks of merge, forecast, data-quality and cost SQL. These prove the design is encoded, not that BigQuery accepts it."""
import re

from sql_helpers import DQ_COLUMNS, EXPECTED, code_only, ddl_columns, no_comments, read


def norm(text):
    return re.sub(r"\s+", " ", no_comments(text))


# ---- fact merge ----
def test_fact_merge_is_keyed_pruned_and_idempotent():
    sql = norm(read("merge", "merge_fact_rides"))
    assert "ON T.ride_id = S.ride_id AND T.pickup_date = S.pickup_date AND T.pickup_date BETWEEN @window_start AND @window_end" in sql
    assert "WHEN MATCHED AND ( T.run_id != S.run_id" in sql  # same run_id and content: no update
    assert "WHEN NOT MATCHED BY TARGET THEN INSERT" in sql
    assert "WHEN NOT MATCHED BY SOURCE AND T.pickup_date BETWEEN @window_start AND @window_end THEN DELETE" in sql
    assert sql.count("ASSERT") == 4 and sql.index("ASSERT") < sql.index("MERGE")


def test_fact_merge_blocks_empty_mixed_or_out_of_window_staging():
    text = read("merge", "merge_fact_rides")
    for message in ("is empty", "different run_id", "outside the window", "duplicate ride_id"):
        assert message in text


# ---- SCD2 merge ----
def test_scd2_merge_preserves_history_and_handles_changes():
    sql = norm(read("merge", "merge_dim_driver_scd2"))
    assert "ON T.driver_id = S.driver_id AND T.effective_start_ts = S.effective_start_ts" in sql
    update = re.search(r"THEN UPDATE SET (.*?) WHEN NOT MATCHED BY TARGET", sql).group(1)
    assert "effective_end_ts = S.effective_end_ts" in update and "is_current = S.is_current" in update  # closing a version
    assert "THEN INSERT" in sql  # opening the new version
    deletes = re.findall(r"WHEN NOT MATCHED BY SOURCE( AND [^;]*?)? THEN DELETE", sql)
    assert len(deletes) == 1 and "T.effective_start_ts >= TIMESTAMP(@snapshot_date, @business_tz)" in deletes[0]
    assert "9999-12-31" in sql


def test_scd2_merge_guards_integrity_before_changing_anything():
    text = read("merge", "merge_dim_driver_scd2")
    for message in ("is empty", "exactly one snapshot_date", "exactly one current version", "duplicate (driver_id, effective_from)",
                    "only the latest snapshot can be (re)applied"):
        assert message in text
    assert text.index("ASSERT") < text.index("MERGE curated_ds.dim_driver_scd2")


def test_rejected_merge_replaces_the_window_in_one_transaction():
    sql = norm(read("merge", "merge_rejected_rides"))
    assert sql.index("BEGIN TRANSACTION") < sql.index("DELETE FROM") < sql.index("INSERT INTO") < sql.index("COMMIT TRANSACTION")
    assert "WHERE event_date BETWEEN @window_start AND @window_end" in sql


# ---- forecast ----
def test_forecast_horizon_is_168_hours_and_hourly():
    train = norm(read("bqml", "02_train_arima_plus"))
    forecast = norm(read("bqml", "03_forecast_168h"))
    assert "STRUCT(168 AS horizon" in forecast
    assert "time_series_id_col" in train and "data_frequency" in train
    raw = read("bqml", "02_train_arima_plus")
    assert "model_type = 'ARIMA_PLUS'" in raw
    assert "data_frequency = 'HOURLY'" in raw and "time_series_id_col = 'pickup_zone'" in raw
    assert "time_series_timestamp_col = 'hour_ts'" in raw and "time_series_data_col = 'rides'" in raw


def test_top_ten_zones_with_low_volume_and_missing_hour_handling():
    text = read("bqml", "01_select_zones_and_series")
    sql = norm(text)
    assert "@top_n BETWEEN 1 AND 10" in sql
    assert "zone_rank <= @top_n" in sql
    assert "LOW_VOLUME" in sql and "TOO_FEW_ACTIVE_HOURS" in sql and "@min_rides" in sql and "@min_active_hours" in sql
    assert "city_hours" in sql and "CROSS JOIN city_hours" in sql and "COALESCE(zh.rides, 0)" in sql
    assert "HAVING SUM(rides) > 0" in sql  # citywide gaps are left out, not zero-filled
    assert "raw_ds" not in sql and "FROM mart_ds.v_zone_demand_hourly" in sql  # trains on the mart view
    assert "curated_ds" not in sql


def test_forecast_output_has_timestamps_and_zone_identifiers():
    cols = dict(ddl_columns(read("ddl", "40_ml_and_ops_tables"), "ml_ds.zone_demand_forecast"))
    for name, col_type in (("as_of_ts", "TIMESTAMP"), ("forecast_ts", "TIMESTAMP"), ("pickup_zone", "STRING"),
                           ("horizon_hour", "INT64"), ("predicted_rides", "FLOAT64"), ("lower_bound", "FLOAT64"),
                           ("upper_bound", "FLOAT64")):
        assert cols[name] == col_type
    insert = re.search(r"INSERT INTO ml_ds\.zone_demand_forecast \((.*?)\)", norm(read("bqml", "03_forecast_168h"))).group(1)
    assert [c.strip() for c in insert.split(",")] == list(cols)
    view = read("marts", "v_zone_demand_forecast")
    assert "f.forecast_ts" in view and "f.pickup_zone" in view and "f.as_of_ts" in view


def test_forecast_rerun_is_delete_then_insert_for_the_same_as_of_ts():
    sql = norm(read("bqml", "03_forecast_168h"))
    assert sql.index("BEGIN TRANSACTION") < sql.index("DELETE FROM ml_ds.zone_demand_forecast WHERE as_of_ts = @as_of_ts") < sql.index("INSERT INTO")
    assert "GREATEST(f.forecast_value, 0)" in sql


def test_forecast_validation_checks_168_rows_start_hour_and_bounds():
    sql = read("bqml", "04_validate_forecast")
    for needle in ("exactly 168 forecast rows", "first forecast hour equals as_of_ts", "contiguous", "non-negative", "!= 168", "!= 167"):
        assert needle in sql
    assert "@run_id AS run_id" in sql


# ---- data quality ----
def test_every_dq_query_returns_the_dq_results_columns_in_order():
    for name in EXPECTED["data_quality"]:
        text = read("data_quality", name)
        final = text[text.rindex("@run_id AS run_id"):]
        positions = [final.index(f"AS {c}") if f"AS {c}" in final else final.index(c) for c in DQ_COLUMNS]
        assert positions == sorted(positions), name
    text = read("bqml", "04_validate_forecast")
    final = text[text.rindex("@run_id AS run_id"):]
    assert [final.index(c) for c in DQ_COLUMNS] == sorted(final.index(c) for c in DQ_COLUMNS)


def test_dq_checks_cover_the_required_categories():
    required = {
        "dq_row_counts": ["DQ-RC-01", "DQ-RC-03", "DQ-RC-04", "rejected_rides", "raw_ds.rides_stream"],
        "dq_null_thresholds": ["COUNTIF(ride_id IS NULL)", "max_rate", "stream_driver_id"],
        "dq_duplicates": ["HAVING COUNT(*) > 1", "COUNTIF(is_current) AS n_current", "LEAD(effective_start_ts)"],
        "dq_negative_fares": ["fare < 0", "DQ-NF-01"],
        "dq_long_trips": ["240", "trip_minutes > max_trip_minutes", "dropoff_ts < pickup_ts"],
        "dq_missing_dimensions": ["zone_match", "driver_match", "DIMENSION_UNAVAILABLE", "NOT_FOUND"],
    }
    for name, needles in required.items():
        text = read("data_quality", name)
        for needle in needles:
            assert needle in text, (name, needle)


def test_error_checks_can_fail_and_warnings_cannot_block():
    for name in EXPECTED["data_quality"]:
        text = read("data_quality", name)
        assert "IF(passed, 'PASS', IF(severity = 'ERROR', 'FAIL', 'WARN'))" in text
        assert "'ERROR'" in text


def test_dq_queries_prune_partitions_by_window():
    for name in ("dq_null_thresholds", "dq_negative_fares", "dq_long_trips", "dq_missing_dimensions"):
        assert "pickup_date BETWEEN @window_start AND @window_end" in read("data_quality", name)


# ---- cost ----
def test_cost_queries_use_jobs_by_project_with_a_time_filter_and_no_double_counting():
    for name in EXPECTED["cost"]:
        sql = norm(read("cost", name))
        assert "`region-asia-south1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT" in sql, name
        assert "total_bytes_billed" in sql and "creation_time >=" in sql, name
        assert "statement_type != 'SCRIPT'" in sql and "error_result IS NULL" in sql, name


def test_daily_bytes_billed_and_expensive_queries():
    daily = norm(read("cost", "cost_daily_bytes_billed"))
    assert "GROUP BY usage_date" in daily and "SUM(total_bytes_billed) AS bytes_billed" in daily
    assert "@price_per_tib" in daily and "INTERVAL @days DAY" in daily
    expensive = norm(read("cost", "cost_expensive_queries"))
    assert "ORDER BY total_bytes_billed DESC LIMIT @top_n" in expensive and "SUBSTR(query, 1, 500)" in expensive


def test_prices_and_budgets_are_parameters_not_literals():
    for name, parameter in ("cost_daily_bytes_billed", "@price_per_tib"), ("cost_expensive_queries", "@price_per_tib"), ("cost_budget_check", "@budget_usd"):
        code = code_only(read("cost", name))
        assert parameter in code
        assert not re.search(r"\b\d+\.\d{2}\b", code), f"price-like literal in {name}"
