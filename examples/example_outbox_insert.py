from typing import Any

from olive_events_bus.outbox import OUTBOX_TABLE_DDL, enqueue_outbox_event


def demo(cur: Any) -> str:
    # ensure table exists (one-time migration should handle this in real usage)
    cur.execute(OUTBOX_TABLE_DDL)

    return enqueue_outbox_event(
        cur,
        topic='olive-identity-provider.user.created',
        event_type='created',
        payload={'user_id': '11111111-1111-1111-1111-111111111111', 'email': 'user@example.com'},
        aggregate_id='11111111-1111-1111-1111-111111111111',
        headers={'correlation_id': 'cor-123'},
    )
