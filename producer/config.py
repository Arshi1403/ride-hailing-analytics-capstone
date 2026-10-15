"""Producer configuration and safety validation."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

PROJECT_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
TOPIC_RE = re.compile(r"^(?!goog)[A-Za-z][A-Za-z0-9._~+%-]{2,254}$")

DEFAULT_PROJECT = "labs-507309"
DEFAULT_TOPIC = "rides-events"
DEFAULT_COUNT = 1000
DEFAULT_DURATION_SECONDS = 600

SAFE_PUBLISH_LIMIT = 50  # real publishing above this needs --confirm-full-run
MAX_COUNT = 5000  # hard cap: this tool is not for production-scale load
MAX_EVENTS_PER_SECOND = 50
MAX_INJECTED_INVALID_WHEN_PUBLISHING = 5


class ConfigError(Exception):
    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("; ".join(problems))


@dataclass(frozen=True)
class ProducerConfig:
    project_id: str = DEFAULT_PROJECT
    topic: str = DEFAULT_TOPIC
    count: int = DEFAULT_COUNT
    duration_seconds: float = DEFAULT_DURATION_SECONDS
    seed: int = 42
    publish: bool = False
    confirm_full_run: bool = False
    inject_invalid: int = 0
    start_time: datetime | None = None
    output_path: str | None = None
    print_sample: int = 3


def validate_config(cfg: ProducerConfig) -> None:
    problems: list[str] = []
    if not PROJECT_RE.match(cfg.project_id):
        problems.append(f"project_id {cfg.project_id!r} is not a valid GCP project id")
    if not TOPIC_RE.match(cfg.topic):
        problems.append(f"topic {cfg.topic!r} is not a valid Pub/Sub topic id (short name, not a full path)")
    if not 1 <= cfg.count <= MAX_COUNT:
        problems.append(f"count must be between 1 and {MAX_COUNT}, got {cfg.count}")
    if cfg.duration_seconds <= 0:
        problems.append("duration_seconds must be > 0")
    elif cfg.count > 0 and cfg.count / cfg.duration_seconds > MAX_EVENTS_PER_SECOND:
        problems.append(f"rate exceeds {MAX_EVENTS_PER_SECOND} events/second; increase duration_seconds")
    if not 0 <= cfg.inject_invalid <= max(cfg.count, 0):
        problems.append("inject_invalid must be between 0 and count")
    if cfg.publish:
        if cfg.count > SAFE_PUBLISH_LIMIT and not cfg.confirm_full_run:
            problems.append(
                f"publishing more than {SAFE_PUBLISH_LIMIT} events needs --confirm-full-run "
                "(only after the user has approved the full run)"
            )
        if cfg.inject_invalid > MAX_INJECTED_INVALID_WHEN_PUBLISHING:
            problems.append(f"at most {MAX_INJECTED_INVALID_WHEN_PUBLISHING} invalid events may be published")
    if problems:
        raise ConfigError(problems)
