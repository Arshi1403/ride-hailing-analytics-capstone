import re

import pytest

from sql_helpers import (
    DATASETS, EXPECTED, SQL, all_files, code_only, header_block, params_declared, params_used, parseable, read,
)

FILES = all_files()
IDS = [f"{folder}/{name}" for folder, name in FILES]


def test_all_expected_files_exist_and_nothing_else():
    found = {p.relative_to(SQL).as_posix() for p in SQL.rglob("*.sql")}
    expected = {f"{folder}/{name}.sql" for folder, name in FILES}
    assert found == expected


@pytest.mark.parametrize("folder,name", FILES, ids=IDS)
def test_header_documents_purpose_params_and_cost(folder, name):
    text = read(folder, name)
    assert text.startswith("-- Purpose:"), "first line must be '-- Purpose: ...'"
    assert re.search(r"^-- Params:", text, flags=re.M)
    assert "approval" in header_block(text, "Cost").lower(), "Cost header must state the approval requirement"


@pytest.mark.parametrize("folder,name", FILES, ids=IDS)
def test_declared_params_match_used_params(folder, name):
    text = read(folder, name)
    assert params_used(text) == params_declared(text)


@pytest.mark.parametrize("folder,name", FILES, ids=IDS)
def test_no_hardcoded_project_and_only_known_datasets(folder, name):
    text = read(folder, name)
    assert "labs-507309" not in text, "project comes from the bq --project_id flag, not from the SQL"
    used = set(re.findall(r"\b(\w+_ds)\b", code_only(text)))
    assert used <= DATASETS, used - DATASETS


@pytest.mark.parametrize("folder,name", FILES, ids=IDS)
def test_syntax_parses_as_bigquery(folder, name):
    """Approximate: catches typos and unbalanced clauses, not BigQuery-specific semantics."""
    sqlglot = pytest.importorskip("sqlglot")
    assert sqlglot.parse(parseable(read(folder, name)), read="bigquery")


def test_no_destructive_statements():
    allowed_replace = {
        ("bqml", "01_select_zones_and_series"): "CREATE OR REPLACE TABLE ml_ds.zone_hourly_training",
        ("bqml", "02_train_arima_plus"): "CREATE OR REPLACE MODEL ml_ds.zone_demand_arima",
    }
    for folder, name in FILES:
        code = code_only(read(folder, name))
        assert not re.search(r"\b(DROP|TRUNCATE)\b", code), f"{folder}/{name}"
        for delete in re.findall(r"DELETE FROM [\w.]+(.*?);", code, flags=re.S):
            assert "WHERE" in delete.upper(), f"unbounded DELETE in {folder}/{name}"
        for target in re.findall(r"CREATE OR REPLACE (?:TABLE|MODEL|VIEW) [\w.]+", code):
            if folder == "marts":
                assert re.fullmatch(r"CREATE OR REPLACE VIEW mart_ds\.v_\w+", target), target
            else:
                assert target == allowed_replace.get((folder, name)), f"{target} in {folder}/{name}"


def test_marts_read_curated_and_ml_only_and_never_select_star():
    for name in EXPECTED["marts"]:
        code = code_only(read("marts", name))
        assert "raw_ds" not in code and "stg_ds" not in code, name
        assert not re.search(r"SELECT\s+\*", code, flags=re.I), name
        assert re.search(rf"CREATE OR REPLACE VIEW mart_ds\.{name}\b", code), name


def test_required_mart_views_exist_with_exact_names():
    for view in ("v_daily_revenue", "v_zone_demand_hourly", "v_driver_leaderboard"):
        assert f"CREATE OR REPLACE VIEW mart_ds.{view} AS" in read("marts", view)
