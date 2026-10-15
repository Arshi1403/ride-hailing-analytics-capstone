import json
from datetime import datetime, timezone

import pytest

from producer.config import ConfigError, ProducerConfig
from producer.generator import INVALID_KINDS, ZONES, corrupt_event, generate_events, inject_invalid
from producer.publisher import DryRunPublisher
from producer.runner import plan_events, run, serialize
from producer.schema import parse_ts, validate_event

START = datetime(2026, 10, 15, 10, 0, 0, tzinfo=timezone.utc)


def test_generation_is_deterministic():
    assert generate_events(50, 7, START, 100) == generate_events(50, 7, START, 100)
    assert generate_events(50, 7, START, 100) != generate_events(50, 8, START, 100)


def test_full_batch_is_valid_unique_and_spread_over_ten_minutes():
    events = generate_events(1000, 42, START, 600)
    assert len(events) == 1000
    assert all(validate_event(e) == [] for e in events)
    assert len({e["ride_id"] for e in events}) == 1000
    first, last = parse_ts(events[0]["pickup_ts"]), parse_ts(events[-1]["pickup_ts"])
    assert (last - first).total_seconds() == pytest.approx(600, abs=1)
    assert all(e["pickup_zone"] in ZONES for e in events)


def test_inject_invalid_corrupts_exact_count_with_known_kinds():
    events = generate_events(20, 1, START, 20)
    positions = inject_invalid(events, 3, 1)
    assert len(positions) == 3
    assert sum(1 for e in events if validate_event(e)) == 3
    assert inject_invalid(generate_events(5, 1, START, 5), 0, 1) == []


@pytest.mark.parametrize("kind", INVALID_KINDS)
def test_each_corruption_is_detected(kind):
    assert validate_event(corrupt_event(generate_events(1, 1, START, 1)[0], kind))


def test_corrupt_unknown_kind():
    with pytest.raises(ValueError):
        corrupt_event({}, "nope")


def test_serialize_is_compact_json():
    data = serialize({"b": 1, "a": 2})
    assert data == b'{"a":2,"b":1}'


def test_dry_run_publishes_nothing_and_never_sleeps():
    sleeps = []
    cfg = ProducerConfig(count=10, duration_seconds=10, start_time=START)
    publisher = DryRunPublisher()
    summary = run(cfg, publisher, sleep=sleeps.append)
    assert summary.mode == "dry-run" and summary.published == 10 and sleeps == []
    assert json.loads(publisher.messages[0][0])["ride_id"]
    assert publisher.messages[0][1]["schema_version"] == "1.0"


def test_publish_mode_paces_events_evenly():
    now = [0.0]
    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    cfg = ProducerConfig(count=5, duration_seconds=10, publish=True, start_time=START)
    run(cfg, DryRunPublisher(), clock=lambda: now[0], sleep=fake_sleep)
    assert sleeps == [2.0, 2.0, 2.0, 2.0]  # event i is sent at i * 2 s; the first is immediate


def test_publish_stats_failures_are_reported():
    class Failing(DryRunPublisher):
        def flush(self):
            from producer.publisher import PublishStats
            return PublishStats(published=3, failed=2, errors=["boom"])

    cfg = ProducerConfig(count=5, duration_seconds=5, start_time=START)
    summary = run(cfg, Failing())
    assert (summary.published, summary.failed, summary.errors) == (3, 2, ["boom"])


def test_run_validates_config_first():
    with pytest.raises(ConfigError):
        run(ProducerConfig(count=0), DryRunPublisher())


def test_plan_refuses_unexpected_invalid_events(monkeypatch):
    monkeypatch.setattr("producer.runner.generate_events",
                        lambda *a: [dict(generate_events(1, 1, START, 1)[0], fare=-1)])
    with pytest.raises(RuntimeError, match="refusing to publish"):
        plan_events(ProducerConfig(count=1, duration_seconds=1))


def test_injected_invalid_events_are_allowed_through():
    cfg = ProducerConfig(count=10, duration_seconds=10, start_time=START, inject_invalid=2)
    events, injected = plan_events(cfg)
    assert len(injected) == 2 and len(events) == 10
