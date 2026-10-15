"""Helpers for reading and lightly parsing the SQL files (no BigQuery access)."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SQL = ROOT / "sql"

EXPECTED = {
    "ddl": ["00_datasets", "10_raw_rides_stream", "11_raw_rides_history", "20_curated_fact_rides",
            "21_curated_dim_driver_scd2", "22_curated_rejected_rides", "30_staging_tables", "40_ml_and_ops_tables"],
    "merge": ["merge_fact_rides", "merge_rejected_rides", "merge_dim_driver_scd2"],
    "marts": ["v_daily_revenue", "v_zone_demand_hourly", "v_driver_leaderboard", "v_zone_demand_forecast"],
    "bqml": ["01_select_zones_and_series", "02_train_arima_plus", "03_forecast_168h", "04_validate_forecast", "05_model_info"],
    "data_quality": ["dq_row_counts", "dq_null_thresholds", "dq_duplicates", "dq_negative_fares", "dq_long_trips",
                     "dq_missing_dimensions"],
    "cost": ["cost_daily_bytes_billed", "cost_expensive_queries", "cost_budget_check"],
}
DATASETS = {"raw_ds", "stg_ds", "curated_ds", "mart_ds", "ml_ds", "ops_ds"}
DQ_COLUMNS = ["run_id", "window_start", "window_end", "check_id", "check_name", "severity", "status",
              "observed_value", "threshold", "detail", "checked_ts"]


def path(folder, name):
    return SQL / folder / f"{name}.sql"


def read(folder, name):
    return path(folder, name).read_text(encoding="utf-8")


def all_files():
    return [(folder, name) for folder, names in EXPECTED.items() for name in names]


def no_comments(text):
    text = re.sub(r"--[^\n]*", "", text)
    return re.sub(r"/\*.*?\*/", "", text, flags=re.S)


def code_only(text):
    """SQL without comments and without string literals (so @names inside messages are ignored)."""
    return re.sub(r"'(?:[^'\\]|\\.)*'", "''", no_comments(text))


def parseable(text):
    """Approximate the statement for sqlglot, which lacks BigQuery ASSERT, ML table functions and INTERVAL expressions."""
    text = re.sub(r"\bASSERT\b.*?;", "", no_comments(text), flags=re.S)
    text = text.replace("INTERVAL @train_days * 24 HOUR", "INTERVAL 1 HOUR")
    while True:
        match = re.search(r"\bML\.\w+\(", text)
        if not match:
            return text
        end = _balanced(text, match.end() - 1)
        text = text[:match.start()] + "ml_stub" + text[match.end() - 1 + len(end) + 2:]


def params_used(text):
    return set(re.findall(r"(?<![\w@])@(\w+)", code_only(text)))


def header_block(text, label):
    """Text of a header section such as 'Params' (until the next '-- Label:' line)."""
    lines, out, active = text.splitlines(), [], False
    for line in lines:
        if not line.startswith("--"):
            if active:
                break
            continue
        if re.match(rf"--\s*{label}:", line):
            active = True
            out.append(line)
        elif active and re.match(r"--\s*[A-Z][A-Za-z ]{2,30}:", line):
            break
        elif active:
            out.append(line)
    return "\n".join(out)


def params_declared(text):
    return set(re.findall(r"@(\w+)", header_block(text, "Params")))


def _balanced(text, start):
    depth, i, quote = 0, start, None
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return text[start + 1:i]
        i += 1
    raise ValueError("unbalanced parentheses")


def _split_top_level(body):
    parts, depth, quote, current = [], 0, None, []
    for ch in body:
        if quote:
            current.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in "'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return [p.strip() for p in parts if p.strip()]


def ddl_columns(text, table):
    """[(name, TYPE)] of CREATE TABLE IF NOT EXISTS <table> (...)."""
    match = re.search(rf"CREATE TABLE IF NOT EXISTS {re.escape(table)}\s*\(", text)
    assert match, f"{table} not found"
    body = _balanced(text, match.end() - 1)
    columns = []
    for part in _split_top_level(re.sub(r"--[^\n]*", "", body)):
        name, col_type = part.split()[:2]
        columns.append((name, col_type))
    return columns


def ddl_tail(text, table):
    match = re.search(rf"CREATE TABLE IF NOT EXISTS {re.escape(table)}\s*\(", text)
    end = match.end() - 1 + len(_balanced(text, match.end() - 1)) + 2
    return text[end:text.index(";", end)]


def statement(text, keyword_regex):
    match = re.search(keyword_regex, code_only(text), flags=re.S)
    assert match, f"{keyword_regex} not found"
    return match.group(0)
