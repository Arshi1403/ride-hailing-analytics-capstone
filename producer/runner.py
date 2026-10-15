"""Plan and run an event batch, pacing publication evenly over the configured duration."""
from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from .config import ProducerConfig, validate_config
from .generator import generate_events, inject_invalid
from .logging_setup import log
from .publisher import Publisher
from .schema import SCHEMA_VERSION, validate_event

logger = logging.getLogger("producer.runner")


@dataclass
class RunSummary:
    run_id: str
    mode: str
    planned: int
    injected_invalid: int
    published: int
    failed: int
    errors: list[str]
    duration_seconds: float


def plan_events(cfg: ProducerConfig) -> tuple[list[dict], list[int]]:
    """Generate the batch. Aborts if any event is invalid that was not deliberately injected."""
    start = cfg.start_time or datetime.now(timezone.utc).replace(microsecond=0)
    events = generate_events(cfg.count, cfg.seed, start, cfg.duration_seconds)
    injected = inject_invalid(events, cfg.inject_invalid, cfg.seed)

    unexpected = [i for i, ev in enumerate(events) if i not in injected and validate_event(ev)]
    if unexpected:
        raise RuntimeError(f"generator produced invalid events at positions {unexpected[:5]}; refusing to publish")
    return events, injected


def serialize(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8")


def run(
    cfg: ProducerConfig,
    publisher: Publisher,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> RunSummary:
    validate_config(cfg)
    run_id = uuid.uuid4().hex[:12]
    events, injected = plan_events(cfg)
    mode = "publish" if cfg.publish else "dry-run"
    log(logger, logging.INFO, "run starting", run_id=run_id, mode=mode, topic=cfg.topic, project_id=cfg.project_id,
        count=cfg.count, duration_seconds=cfg.duration_seconds, seed=cfg.seed, injected_invalid=len(injected))

    attributes = {"schema_version": SCHEMA_VERSION, "producer_run_id": run_id}
    interval = cfg.duration_seconds / cfg.count
    start = clock()
    for i, event in enumerate(events):
        if cfg.publish:  # dry-run is not paced
            delay = start + i * interval - clock()
            if delay > 0:
                sleep(delay)
        publisher.publish(serialize(event), attributes)
        if (i + 1) % 100 == 0:
            log(logger, logging.INFO, "progress", run_id=run_id, sent=i + 1, total=cfg.count)

    stats = publisher.flush()
    summary = RunSummary(run_id, mode, cfg.count, len(injected), stats.published, stats.failed, stats.errors,
                         round(clock() - start, 3))
    log(logger, logging.INFO if not stats.failed else logging.ERROR, "run finished", **summary.__dict__)
    return summary
