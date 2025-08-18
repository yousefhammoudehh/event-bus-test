from __future__ import annotations

from prometheus_client import Counter


class _Metrics:
    def __init__(self) -> None:
        self.events_published = Counter(
            "olive_events_published_total",
            "Total number of events published",
            labelnames=["topic", "service", "version"],
        )
        self.events_consumed = Counter(
            "olive_events_consumed_total",
            "Total number of events consumed",
            labelnames=["topic", "service", "group"],
        )
        self.events_failed = Counter(
            "olive_events_failed_total",
            "Total number of events failed and sent to DLQ",
            labelnames=["topic", "service", "group"],
        )


metrics = _Metrics()
