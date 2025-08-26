#!/usr/bin/env python3
from __future__ import annotations

import os

from olive_events_bus.admin.topics import TopicSpec, create_topics

# Example topic specs. Adjust partitions and replication for prod.
TOPICS = [
    TopicSpec(
        name='olive-identity-provider.user.created',
        partitions=3,
        replication_factor=3,
        retention_ms=7 * 24 * 3600 * 1000,
    ),
    TopicSpec(name='olive-leaves.leave.requested', partitions=6, replication_factor=3, retention_ms=24 * 3600 * 1000),
    TopicSpec(
        name='olive-leaves.leave.requested.dlq', partitions=3, replication_factor=3, retention_ms=14 * 24 * 3600 * 1000
    ),
]


def main() -> None:
    bs = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    create_topics(bs, TOPICS)


if __name__ == '__main__':
    main()
