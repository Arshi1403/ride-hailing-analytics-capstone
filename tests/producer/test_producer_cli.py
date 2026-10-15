import json

import pytest

from producer.cli import main
from producer.config import ConfigError, ProducerConfig, validate_config


def problems(**kwargs):
    with pytest.raises(ConfigError) as info:
        validate_config(ProducerConfig(**kwargs))
    return " | ".join(info.value.problems)


def test_defaults_are_a_safe_dry_run():
    cfg = ProducerConfig()
    validate_config(cfg)
    assert (cfg.project_id, cfg.topic, cfg.count, cfg.duration_seconds, cfg.publish) == (
        "labs-507309", "rides-events", 1000, 600, False)


def test_full_publish_needs_explicit_confirmation():
    assert "--confirm-full-run" in problems(publish=True)
    validate_config(ProducerConfig(publish=True, confirm_full_run=True))


def test_small_publish_needs_no_confirmation():
    validate_config(ProducerConfig(publish=True, count=10, duration_seconds=10))
    validate_config(ProducerConfig(publish=True, count=50, duration_seconds=50))
    assert "--confirm-full-run" in problems(publish=True, count=51, duration_seconds=51)


@pytest.mark.parametrize("kwargs,fragment", [
    ({"project_id": "Bad_Project"}, "project_id"),
    ({"topic": "projects/p/topics/t"}, "topic"),
    ({"topic": "goog-reserved"}, "topic"),
    ({"count": 0}, "count"),
    ({"count": 5001, "duration_seconds": 5001}, "count"),
    ({"duration_seconds": 0}, "duration_seconds"),
    ({"count": 1000, "duration_seconds": 10}, "rate"),
    ({"inject_invalid": 2000}, "inject_invalid"),
    ({"publish": True, "count": 10, "duration_seconds": 10, "inject_invalid": 6}, "at most 5"),
])
def test_invalid_configs(kwargs, fragment):
    assert fragment in problems(**kwargs)


def test_cli_dry_run_prints_samples_and_summary(capsys):
    assert main(["--count", "10", "--duration-seconds", "10", "--print-sample", "2"]) == 0
    out = capsys.readouterr().out
    assert out.count('"ride_id"') == 2
    summary = json.loads(out[out.index("{\n"):])
    assert summary["mode"] == "dry-run" and summary["published"] == 10


def test_cli_writes_jsonl_output(tmp_path, capsys):
    target = tmp_path / "events.jsonl"
    assert main(["--count", "12", "--duration-seconds", "12", "--output", str(target)]) == 0
    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 12 and json.loads(lines[0])["pickup_zone"]


def test_cli_config_error_exit_code_2(capsys):
    assert main(["--publish"]) == 2
    assert "confirm-full-run" in capsys.readouterr().err


def test_cli_bad_start_time_exit_code_2(capsys):
    assert main(["--start-time", "yesterday"]) == 2


def test_cli_reads_env(monkeypatch, capsys):
    monkeypatch.setenv("PRODUCER_COUNT", "7")
    monkeypatch.setenv("PRODUCER_DURATION_SECONDS", "7")
    assert main([]) == 0
    assert '"planned": 7' in capsys.readouterr().out
