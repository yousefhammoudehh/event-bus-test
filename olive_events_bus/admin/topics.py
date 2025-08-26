from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

try:
    from kafka.admin import KafkaAdminClient, NewTopic
except Exception:  # pragma: no cover - optional dependency
    KafkaAdminClient = None
    NewTopic = None


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int | None = None


def create_topics(bootstrap_servers: str, topics: Iterable[TopicSpec]) -> None:
    if KafkaAdminClient is None or NewTopic is None:
        raise RuntimeError("kafka-python-ng not installed; install with extras 'admin'")
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        new_topics: list[object] = [
            NewTopic(
                name=t.name,
                num_partitions=t.partitions,
                replication_factor=t.replication_factor,
                topic_configs={'retention.ms': str(t.retention_ms)} if t.retention_ms else None,
            )
            for t in topics
        ]
        admin.create_topics(new_topics=new_topics, validate_only=False)
    finally:
        admin.close()
