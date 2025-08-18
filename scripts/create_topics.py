#!/usr/bin/env python3
from __future__ import annotations

from olive_events_bus.admin.topics import TopicSpec, create_topics

# Example topic specs. Adjust partitions and replication for prod.
TOPICS = [
    TopicSpec(
        name="identity-provider.user.created.v1",
        partitions=3,
        replication_factor=3,
        retention_ms=7 * 24 * 3600 * 1000,
    ),
    TopicSpec(
        name="leaves.leave.requested.v1",
        partitions=6,
        replication_factor=3,
        retention_ms=24 * 3600 * 1000,
    ),
    TopicSpec(
        name="leaves.leave.requested.v1.dlq",
        partitions=3,
        replication_factor=3,
        retention_ms=14 * 24 * 3600 * 1000,
    ),
]


def main() -> None:
    import os

    bs = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    create_topics(bs, TOPICS)


if __name__ == "__main__":
    main()
