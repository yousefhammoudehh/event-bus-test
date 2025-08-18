from olive_events_bus.outbox import enqueue_outbox_event, OUTBOX_TABLE_DDL

def demo(cur):
    # ensure table exists (one-time migration should handle this in real usage)
    cur.execute(OUTBOX_TABLE_DDL)

    event_id = enqueue_outbox_event(
        cur,
        topic="identity-provider.user.created.v1",
        event_type="created",
        payload={"user_id": "11111111-1111-1111-1111-111111111111", "email": "user@example.com"},
        aggregate_id="11111111-1111-1111-1111-111111111111",
        headers={"correlation_id": "cor-123"},
    )
    return event_id
