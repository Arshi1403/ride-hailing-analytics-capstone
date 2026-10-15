from datetime import date

import pytest

from ridespark.scd2 import END_OF_TIME, Scd2Error, apply_snapshot, empty_history, prepare_snapshot, validate_scd2
from ridespark.schemas import DRIVER_SCD2_SCHEMA
from spark_helpers import D1, D2, D3, make_df, settle, snapshot_df


def versions(df):
    return sorted(
        (r.driver_id, r.effective_from, r.effective_to, r.is_current, r.vehicle_type, r.home_zone, r.status)
        for r in df.collect()
    )


def apply(spark, history, snapshot_rows, when):
    out = settle(spark, apply_snapshot(history, snapshot_df(spark, *snapshot_rows), when))
    validate_scd2(out)
    return out


def first_load(spark):
    return apply(spark, empty_history(spark), [
        ("D1", "SEDAN", "1", "ACTIVE"), ("D2", "MINI", "2", "ACTIVE"), ("D3", "SUV", "1", "ACTIVE")], D1)


def test_new_drivers_get_one_current_version_each(spark):
    out = first_load(spark)
    assert versions(out) == [
        ("D1", D1, END_OF_TIME, True, "SEDAN", "1", "ACTIVE"),
        ("D2", D1, END_OF_TIME, True, "MINI", "2", "ACTIVE"),
        ("D3", D1, END_OF_TIME, True, "SUV", "1", "ACTIVE"),
    ]
    assert [(f.name, f.dataType.simpleString()) for f in out.schema.fields] == \
        [(f.name, f.dataType.simpleString()) for f in DRIVER_SCD2_SCHEMA.fields]


def test_unchanged_records_create_no_new_versions(spark):
    h1 = first_load(spark)
    h2 = apply(spark, h1, [("D1", "SEDAN", "1", "ACTIVE"), ("D2", "MINI", "2", "ACTIVE"), ("D3", "SUV", "1", "ACTIVE")], D2)
    assert versions(h2) == versions(h1)


def test_changed_attribute_closes_old_version_and_opens_new_one(spark):
    h2 = apply(spark, first_load(spark), [
        ("D1", "SUV", "1", "ACTIVE"), ("D2", "MINI", "2", "ACTIVE"), ("D3", "SUV", "1", "ACTIVE")], D2)
    d1 = [v for v in versions(h2) if v[0] == "D1"]
    assert d1 == [
        ("D1", D1, D2, False, "SEDAN", "1", "ACTIVE"),  # prior version preserved, ends where the next begins
        ("D1", D2, END_OF_TIME, True, "SUV", "1", "ACTIVE"),
    ]
    assert len([v for v in versions(h2) if v[0] == "D2"]) == 1


@pytest.mark.parametrize("column,new_row", [
    ("status", ("D1", "SEDAN", "1", "INACTIVE")),
    ("home_zone", ("D1", "SEDAN", "2", "ACTIVE")),
    ("vehicle_type", ("D1", "MINI", "1", "ACTIVE")),
])
def test_each_tracked_attribute_triggers_a_new_version(spark, column, new_row):
    h = apply(spark, first_load(spark), [new_row], D2)
    assert len([v for v in versions(h) if v[0] == "D1"]) == 2


def test_new_driver_on_a_later_day_is_added_without_touching_others(spark):
    h1 = first_load(spark)
    h2 = apply(spark, h1, [("D9", "BIKE", "2", "ACTIVE")], D2)
    assert [v for v in versions(h2) if v[0] != "D9"] == versions(h1)
    assert ("D9", D2, END_OF_TIME, True, "BIKE", "2", "ACTIVE") in versions(h2)


def test_drivers_missing_from_a_snapshot_stay_current(spark):
    h2 = apply(spark, first_load(spark), [("D1", "SEDAN", "1", "ACTIVE")], D2)
    assert versions(h2) == versions(first_load(spark))


def test_repeating_the_same_snapshot_is_a_no_op(spark):
    rows = [("D1", "SUV", "1", "ACTIVE"), ("D2", "MINI", "2", "ACTIVE"), ("D3", "SUV", "1", "ACTIVE")]
    h2 = apply(spark, first_load(spark), rows, D2)
    again = apply(spark, h2, rows, D2)
    thrice = apply(spark, again, rows, D2)
    assert versions(again) == versions(h2) == versions(thrice)


def test_rerun_of_the_latest_date_with_corrected_data_replaces_that_days_changes(spark):
    h1 = first_load(spark)
    h2 = apply(spark, h1, [("D1", "SUV", "1", "ACTIVE")], D2)
    corrected = apply(spark, h2, [("D1", "MINI", "1", "ACTIVE")], D2)
    d1 = [v for v in versions(corrected) if v[0] == "D1"]
    assert d1 == [("D1", D1, D2, False, "SEDAN", "1", "ACTIVE"), ("D1", D2, END_OF_TIME, True, "MINI", "1", "ACTIVE")]
    reverted = apply(spark, h2, [("D1", "SEDAN", "1", "ACTIVE")], D2)  # change undone: back to one version
    assert [v for v in versions(reverted) if v[0] == "D1"] == [("D1", D1, END_OF_TIME, True, "SEDAN", "1", "ACTIVE")]


def test_several_days_build_a_contiguous_history(spark):
    h = first_load(spark)
    h = apply(spark, h, [("D1", "SUV", "1", "ACTIVE")], D2)
    h = apply(spark, h, [("D1", "SUV", "1", "SUSPENDED")], D3)
    d1 = [v for v in versions(h) if v[0] == "D1"]
    assert [(v[1], v[2], v[3]) for v in d1] == [(D1, D2, False), (D2, D3, False), (D3, END_OF_TIME, True)]


def test_exactly_one_current_record_per_driver(spark):
    h = first_load(spark)
    for day, vehicle in ((D2, "SUV"), (D3, "MINI")):
        h = apply(spark, h, [("D1", vehicle, "1", "ACTIVE"), ("D4", "BIKE", "2", "ACTIVE")], day)
    per_driver = {}
    for row in h.collect():
        per_driver[row.driver_id] = per_driver.get(row.driver_id, 0) + int(row.is_current)
    assert set(per_driver.values()) == {1}


def test_older_snapshot_than_history_is_refused(spark):
    h2 = apply(spark, first_load(spark), [("D1", "SUV", "1", "ACTIVE")], D2)
    with pytest.raises(Scd2Error, match="after snapshot date"):
        apply_snapshot(h2, snapshot_df(spark, ("D1", "SEDAN", "1", "ACTIVE")), D1)


def test_null_attributes_are_compared_null_safely(spark):
    h1 = apply(spark, empty_history(spark), [("D1", "SEDAN", None, "ACTIVE")], D1)
    h2 = apply(spark, h1, [("D1", "SEDAN", None, "ACTIVE")], D2)
    assert versions(h2) == versions(h1)
    h3 = apply(spark, h2, [("D1", "SEDAN", "1", "ACTIVE")], D3)
    assert len(versions(h3)) == 2


def test_conflicting_or_invalid_snapshot_rows_are_ignored_and_reported(spark):
    snap = snapshot_df(spark, ("D1", "SEDAN", "1", "ACTIVE"), ("D1", "SUV", "1", "ACTIVE"),
                       ("D2", "MINI", "2", "ACTIVE"), ("D2", "MINI", "2", "ACTIVE"), (None, "X", "1", "ACTIVE"),
                       ("  ", "Y", "1", "ACTIVE"))
    clean, conflicts = prepare_snapshot(snap)
    assert [r.driver_id for r in clean.collect()] == ["D2"]
    assert sorted((r.conflict_reason for r in conflicts.collect())) == \
        ["CONFLICTING_SNAPSHOT_ROWS", "CONFLICTING_SNAPSHOT_ROWS", "NULL_DRIVER_ID", "NULL_DRIVER_ID"]


def test_conflicting_driver_keeps_its_existing_version(spark):
    h = apply(spark, first_load(spark), [("D1", "SEDAN", "1", "ACTIVE"), ("D1", "SUV", "1", "ACTIVE")], D2)
    assert versions(h) == versions(first_load(spark))


def test_validate_detects_broken_history(spark):
    two_current = make_df(spark, [
        ("D1", "A", "1", "ACTIVE", "h1", D1, END_OF_TIME, True),
        ("D1", "B", "1", "ACTIVE", "h2", D2, END_OF_TIME, True)], DRIVER_SCD2_SCHEMA)
    with pytest.raises(Scd2Error, match="exactly one current"):
        validate_scd2(two_current)
    gap = make_df(spark, [
        ("D1", "A", "1", "ACTIVE", "h1", D1, date(2026, 10, 2), False),
        ("D1", "B", "1", "ACTIVE", "h2", D3, END_OF_TIME, True)], DRIVER_SCD2_SCHEMA)
    with pytest.raises(Scd2Error, match="overlap or leave a gap"):
        validate_scd2(gap)
    no_current = make_df(spark, [
        ("D1", "A", "1", "ACTIVE", "h1", D1, D2, False)], DRIVER_SCD2_SCHEMA)
    with pytest.raises(Scd2Error):
        validate_scd2(no_current)
