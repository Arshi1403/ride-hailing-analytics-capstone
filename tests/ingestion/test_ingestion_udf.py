"""Checks the JavaScript UDF stays in sync with producer/schema.py; behaviour tests need Node.js."""
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from producer import schema
from producer.generator import INVALID_KINDS, corrupt_event, generate_events
from datetime import datetime, timezone

UDF = Path(__file__).resolve().parents[2] / "ingestion" / "udf" / "ride_event_udf.js"
NODE = shutil.which("node")


def constant(name):
    return float(re.search(rf"var {name} = ([0-9.]+);", UDF.read_text(encoding="utf-8")).group(1))


def test_udf_constants_match_python_rules():
    assert constant("FARE_MAX") == schema.FARE_MAX
    assert constant("SURGE_MIN") == schema.SURGE_MIN
    assert constant("SURGE_MAX") == schema.SURGE_MAX
    assert constant("MAX_TRIP_SECONDS") == schema.MAX_TRIP_SECONDS


def test_udf_lists_the_same_fields():
    text = UDF.read_text(encoding="utf-8")
    for name in schema.FIELDS:
        assert f'"{name}"' in text


def test_udf_is_es5_only():
    text = UDF.read_text(encoding="utf-8")
    assert not re.search(r"\b(let|const)\b|=>|`", re.sub(r"/\*.*?\*/", "", text, flags=re.S))


def run_udf(payload: dict):
    script = (
        "const vm=require('vm');const fs=require('fs');const ctx={};vm.createContext(ctx);"
        f"vm.runInContext(fs.readFileSync({json.dumps(str(UDF))},'utf8'),ctx);"
        "let out;try{out={ok:ctx.process(process.argv[1])}}catch(e){out={error:String(e.message)}}"
        "console.log(JSON.stringify(out))"
    )
    done = subprocess.run([NODE, "-e", script, json.dumps(payload)], capture_output=True, text=True, check=True)
    return json.loads(done.stdout)


@pytest.mark.skipif(NODE is None, reason="Node.js not installed; UDF behaviour not executed")
def test_udf_adds_ingest_ts_to_valid_events():
    event = generate_events(1, 1, datetime(2026, 10, 15, tzinfo=timezone.utc), 1)[0]
    row = json.loads(run_udf(event)["ok"])
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", row["ingest_ts"])
    assert {k: v for k, v in row.items() if k != "ingest_ts"} == event


@pytest.mark.skipif(NODE is None, reason="Node.js not installed; UDF behaviour not executed")
@pytest.mark.parametrize("kind", INVALID_KINDS)
def test_udf_rejects_what_python_rejects(kind):
    event = corrupt_event(generate_events(1, 1, datetime(2026, 10, 15, tzinfo=timezone.utc), 1)[0], kind)
    assert "RIDE_EVENT_INVALID" in run_udf(event)["error"]
