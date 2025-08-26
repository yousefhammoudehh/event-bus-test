from __future__ import annotations

import typing
import uuid

# --- CDC Outbox (Debezium) helpers ---

# Recommended table for Debezium Outbox Event Router SMT
# See infrastructure/debezium/postgres-outbox-connector.json
OUTBOX_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS public.event_outbox (
    id            UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,  -- we store the FULL Kafka topic here (service.entity.event)
    aggregate_id   TEXT NULL,      -- optional key for partitioning
    type           TEXT NOT NULL,  -- logical event type (e.g., created)
    payload        JSONB NOT NULL, -- event envelope or payload
    headers        JSONB NULL,     -- optional headers
    timestamp      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_outbox_ts ON public.event_outbox (timestamp);
"""


def enqueue_outbox_event(
    cur: typing.Any,
    *,
    topic: str,
    event_type: str,
    payload: dict[str, typing.Any],
    aggregate_id: str | None = None,
    headers: dict[str, typing.Any] | None = None,
    event_id: str | None = None,
) -> str:
    """Insert an outbox row within the caller's DB transaction (CDC-friendly).

    Use this in the same transaction that persists your domain change so CDC (Debezium)
    will atomically capture both. The Debezium Outbox Event Router will route the record
    to the Kafka topic stored in aggregate_type.

    Args:
        cur: psycopg cursor within an open transaction
        topic: full Kafka topic name (service.entity.event)
        event_type: logical event type (e.g., created)
        payload: event payload (already validated)
        aggregate_id: optional key used as Kafka message key
        headers: optional headers stored for routing or enrichment
        event_id: optional UUID; generated if not provided

    Returns:
        The event_id (UUID string) used for the outbox row
    """
    eid = event_id or str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO public.event_outbox (id, aggregate_type, aggregate_id, type, payload, headers)
        VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb)
        """,
        (eid, topic, aggregate_id, event_type, payload, headers),
    )
    return eid
