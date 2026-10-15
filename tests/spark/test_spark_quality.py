from decimal import Decimal

import pytest

from ridespark.quality import MAX_TRIP_SECONDS, add_reject_reason, dedupe_by_ride_id, split_valid_rejected
from spark_helpers import unified_df, unified_row, utc


def reasons(spark, **overrides):
    return add_reject_reason(unified_df(spark, unified_row(**overrides))).first().reject_reason


def test_valid_row_has_no_reject_reason(spark):
    assert reasons(spark) is None


@pytest.mark.parametrize("overrides,expected", [
    (dict(pickup_ts=None), "NULL_PICKUP_TS"),
    (dict(dropoff_ts=None), "NULL_DROPOFF_TS"),
    (dict(ride_id=None), "NULL_RIDE_ID"),
    (dict(fare=Decimal("-0.01")), "NEGATIVE_FARE"),
    (dict(fare=None), "NULL_FARE"),
    (dict(dropoff_ts=utc(2026, 10, 14, 9, 59)), "NEGATIVE_TRIP_DURATION"),
])
def test_invalid_rows_get_a_reason(spark, overrides, expected):
    assert reasons(spark, **overrides) == expected


def test_zero_fare_and_zero_minute_trip_are_valid(spark):
    assert reasons(spark, fare=Decimal("0")) is None
    assert reasons(spark, dropoff_ts=utc(2026, 10, 14, 10, 0)) is None


def test_four_hour_limit_is_exclusive_of_exactly_four_hours(spark):
    assert MAX_TRIP_SECONDS == 14400
    assert reasons(spark, dropoff_ts=utc(2026, 10, 14, 14, 0, 0)) is None
    assert reasons(spark, dropoff_ts=utc(2026, 10, 14, 14, 0, 1)) == "TRIP_TOO_LONG"


def test_first_matching_reason_wins(spark):
    assert reasons(spark, pickup_ts=None, fare=Decimal("-1")) == "NULL_PICKUP_TS"


def test_split_separates_valid_and_rejected(spark):
    valid, rejected = split_valid_rejected(unified_df(
        spark, unified_row(ride_id="ok"), unified_row(ride_id="bad", fare=Decimal("-3"))))
    assert [r.ride_id for r in valid.collect()] == ["ok"]
    assert [(r.ride_id, r.reject_reason) for r in rejected.collect()] == [("bad", "NEGATIVE_FARE")]
    assert "reject_reason" not in valid.columns


def test_dedupe_keeps_latest_ingest_ts(spark):
    out = dedupe_by_ride_id(unified_df(
        spark,
        unified_row(fare=Decimal("10"), ingest_ts=utc(2026, 10, 14, 11)),
        unified_row(fare=Decimal("20"), ingest_ts=utc(2026, 10, 14, 12)),
        unified_row(fare=Decimal("30"), ingest_ts=utc(2026, 10, 14, 9)),
    )).collect()
    assert len(out) == 1 and out[0].fare == Decimal("20.000000000")


def test_dedupe_null_ingest_ts_loses_to_a_real_one(spark):
    out = dedupe_by_ride_id(unified_df(
        spark, unified_row(fare=Decimal("1"), ingest_ts=None), unified_row(fare=Decimal("2")))).collect()
    assert [r.fare for r in out] == [Decimal("2")]


def test_dedupe_tie_break_prefers_stream_then_source_file_then_content(spark):
    same = utc(2026, 10, 14, 12)
    stream_vs_hist = dedupe_by_ride_id(unified_df(
        spark,
        unified_row(source="historical", source_file="gs://b/a.csv", fare=Decimal("1"), ingest_ts=same),
        unified_row(source="stream", fare=Decimal("2"), ingest_ts=same))).first()
    assert stream_vs_hist.source == "stream"

    by_file = dedupe_by_ride_id(unified_df(
        spark,
        unified_row(source="historical", source_file="gs://b/a.csv", ingest_ts=same),
        unified_row(source="historical", source_file="gs://b/b.csv", ingest_ts=same))).first()
    assert by_file.source_file == "gs://b/b.csv"

    by_content = dedupe_by_ride_id(unified_df(
        spark, unified_row(fare=Decimal("1"), ingest_ts=same), unified_row(fare=Decimal("2"), ingest_ts=same))).collect()
    assert len(by_content) == 1


def test_dedupe_result_does_not_depend_on_input_order(spark):
    rows = [unified_row(fare=Decimal(str(i)), ingest_ts=utc(2026, 10, 14, 12)) for i in range(1, 6)]
    forward = dedupe_by_ride_id(unified_df(spark, *rows)).first().fare
    backward = dedupe_by_ride_id(unified_df(spark, *reversed(rows))).first().fare
    assert forward == backward


def test_dedupe_is_per_ride_and_keeps_null_ride_ids_for_rejection(spark):
    out = dedupe_by_ride_id(unified_df(
        spark, unified_row(ride_id="a"), unified_row(ride_id="b"),
        unified_row(ride_id=None), unified_row(ride_id=None, fare=Decimal("9")))).collect()
    assert sorted(r.ride_id or "null" for r in out) == ["a", "b", "null", "null"]
