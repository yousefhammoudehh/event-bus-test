# CDC Outbox with Postgres + Debezium

This guide describes the recommended CDC-based outbox pattern.

- Create the outbox table (see DDL below) in each service DB.
- In the same transaction that mutates domain state, insert an outbox row using `enqueue_outbox_event(...)`.
- Debezium connector streams outbox rows and routes them to Kafka topics via the Event Router SMT.

## Outbox Table DDL

```sql
CREATE TABLE IF NOT EXISTS public.event_outbox (
    id             UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NULL,
    type           TEXT NOT NULL,
    payload        JSONB NOT NULL,
    headers        JSONB NULL,
    timestamp      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_outbox_ts ON public.event_outbox (timestamp);
```

## Insert in Same Transaction

```python
from olive_events_bus.outbox import enqueue_outbox_event

with conn:  # psycopg transaction
    with conn.cursor() as cur:
        # 1) write your domain rows here
        # 2) queue outbox row
        enqueue_outbox_event(
            cur,
            topic="identity-provider.user.created.v1",
            event_type="created",
            payload={"user_id": "...", "email": "..."},
            aggregate_id="..."
        )
```

## Debezium Connector

See `infrastructure/debezium/postgres-outbox-connector.json` for a ready-to-apply connector config using the Outbox Event Router transform.

- `aggregate_type` routes to the Kafka topic.
- `aggregate_id` becomes the message key (header id), ensuring partition affinity.
- `payload` is the message value; store either the full envelope or just the payload depending on your needs.

## Why CDC Outbox

- Atomic with DB changes (no dual-write inconsistency)
- Backpressure-tolerant, replayable
- Clear separation of concerns (service writes; Debezium streams)
