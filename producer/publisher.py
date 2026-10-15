"""Publishing back ends: a dry-run collector and the real Pub/Sub client."""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Protocol

from .logging_setup import log

logger = logging.getLogger("producer.publisher")


@dataclass
class PublishStats:
    published: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


class Publisher(Protocol):
    def publish(self, data: bytes, attributes: dict[str, str]) -> None: ...

    def flush(self) -> PublishStats: ...


class DryRunPublisher:
    """Collects messages in memory; nothing leaves the machine."""

    def __init__(self) -> None:
        self.messages: list[tuple[bytes, dict[str, str]]] = []

    def publish(self, data: bytes, attributes: dict[str, str]) -> None:
        self.messages.append((data, attributes))

    def flush(self) -> PublishStats:
        return PublishStats(published=len(self.messages))


class PubSubPublisher:
    """Publishes to projects/{project}/topics/{topic} using Application Default Credentials."""

    def __init__(self, project_id: str, topic: str, result_timeout: float = 60.0) -> None:
        try:
            from google.cloud import pubsub_v1
        except ImportError as exc:
            raise RuntimeError("google-cloud-pubsub is not installed. Run: pip install -r producer/requirements.txt") from exc
        self._client = pubsub_v1.PublisherClient()
        self._topic_path = self._client.topic_path(project_id, topic)
        self._timeout = result_timeout
        self._futures = []
        self._lock = threading.Lock()
        self._stats = PublishStats()

    def publish(self, data: bytes, attributes: dict[str, str]) -> None:
        future = self._client.publish(self._topic_path, data, **attributes)
        self._futures.append(future)

    def flush(self) -> PublishStats:
        for future in self._futures:
            try:
                future.result(timeout=self._timeout)
                self._stats.published += 1
            except Exception as exc:  # noqa: BLE001 - report every failure, keep draining
                self._stats.failed += 1
                if len(self._stats.errors) < 5:
                    self._stats.errors.append(f"{type(exc).__name__}: {exc}")
        self._futures.clear()
        log(logger, logging.INFO, "publish flush complete", topic_path=self._topic_path,
            published=self._stats.published, failed=self._stats.failed)
        return self._stats
