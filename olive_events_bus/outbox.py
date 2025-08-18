from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional
import uuid

try:
    from psycopg import connect  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    connect = None  # type: ignore


@dataclass
class OutboxEvent:
    id: int
    topic: str
    payload: Dict[str, Any]


class PostgresOutboxReader:
    """Simple polling reader for an outbox table (CDC-friendly).

    Expected table schema (minimum):
      - id BIGSERIAL PRIMARY KEY
      - topic TEXT NOT NULL
      - payload JSONB NOT NULL
      - published_at TIMESTAMPTZ NULL
    """

    def __init__(self, dsn: str, table: str = "event_outbox"):
        if connect is None:
            raise RuntimeError("psycopg is not installed; install with extras 'outbox'")
        self.dsn = dsn
        self.table = table

    def fetch_batch(self, limit: int = 100) -> Iterable[OutboxEvent]:
        with connect(self.dsn) as conn:  # type: ignore
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, topic, payload
                    FROM {self.table}
                    WHERE published_at IS NULL
                    ORDER BY id
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    (limit,),
                )
                for row in cur.fetchall():
                    yield OutboxEvent(id=row[0], topic=row[1], payload=row[2])

    def mark_published(self, ids: list[int]) -> None:
        if not ids:
            return
        with connect(self.dsn) as conn:  # type: ignore
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {self.table} SET published_at = NOW() WHERE id = ANY(%s)",
                    (ids,),
                )
                conn.commit()


# --- CDC Outbox (Debezium) helpers ---

# Recommended table for Debezium Outbox Event Router SMT
# See infrastructure/debezium/postgres-outbox-connector.json
OUTBOX_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS public.event_outbox (
    id            UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,  -- we store the FULL Kafka topic here (service.entity.event.version)
    aggregate_id   TEXT NULL,      -- optional key for partitioning
    type           TEXT NOT NULL,  -- logical event type (e.g., created)
    payload        JSONB NOT NULL, -- event envelope or payload
    headers        JSONB NULL,     -- optional headers
    timestamp      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_outbox_ts ON public.event_outbox (timestamp);
"""


def enqueue_outbox_event(
    cur,
    *,
    topic: str,
    event_type: str,
    payload: Dict[str, Any],
    aggregate_id: Optional[str] = None,
    headers: Optional[Dict[str, Any]] = None,
    event_id: Optional[str] = None,
) -> str:
    """Insert an outbox row within the caller's DB transaction (CDC-friendly).

    Use this in the same transaction that persists your domain change so CDC (Debezium)
    will atomically capture both. The Debezium Outbox Event Router will route the record
    to the Kafka topic stored in aggregate_type.

    Args:
        cur: psycopg cursor within an open transaction
        topic: full Kafka topic name (service.entity.event.version)
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
        (
            eid,
            topic,
            aggregate_id,
            event_type,
            payload,
            headers,
        ),
    )
    return eid


# --- Optional: SQLAlchemy Core integration ---
try:  # pragma: no cover - optional dependency
    from sqlalchemy import (
        Table, Column, MetaData, text,
        String, JSON, DateTime, BigInteger, select, update, func
    )
    from sqlalchemy.dialects.postgresql import UUID as PG_UUID
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.engine import Connection
    from sqlalchemy.engine import Engine

    metadata = MetaData()
    # CDC outbox table (UUID id, Debezium Outbox Event Router)
    event_outbox_table = Table(
        "event_outbox",
        metadata,
        Column("id", PG_UUID(as_uuid=False), primary_key=True),
        Column("aggregate_type", String, nullable=False),
        Column("aggregate_id", String, nullable=True),
        Column("type", String, nullable=False),
        Column("payload", JSON, nullable=False),
        Column("headers", JSON, nullable=True),
        Column("timestamp", DateTime(timezone=True), server_default=text("NOW()"), nullable=False),
        schema="public",
    )

    # Polling outbox table (BIGSERIAL id, published_at flag) for non-CDC environments
    polling_outbox_table = Table(
        "event_outbox",
        metadata,
        Column("id", BigInteger, primary_key=True),
        Column("topic", String, nullable=False),
        Column("payload", JSONB, nullable=False),
        Column("published_at", DateTime(timezone=True), nullable=True),
        schema="public",
    )

    def sa_enqueue_outbox_event(
        conn: Connection,
        *,
        topic: str,
        event_type: str,
        payload: Dict[str, Any],
        aggregate_id: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        event_id: Optional[str] = None,
    ) -> str:
        """Insert an outbox row using SQLAlchemy Core within the current transaction."""
        eid = event_id or str(uuid.uuid4())
        conn.execute(
            event_outbox_table.insert().values(
                id=eid,
                aggregate_type=topic,
                aggregate_id=aggregate_id,
                type=event_type,
                payload=payload,
                headers=headers,
            )
        )
        return eid

    class SAOutboxReader:
        """SQLAlchemy Core-based polling reader (parity with PostgresOutboxReader).

        Uses the polling-style outbox table with columns: id, topic, payload, published_at.
        Intended for environments without Debezium/CDC.
        """

        def __init__(self, conn_or_engine: Connection | Engine, table: Table | None = None) -> None:
            self.conn_or_engine = conn_or_engine
            self.table = table or polling_outbox_table

        def _conn(self) -> Connection:
            if hasattr(self.conn_or_engine, "execute") and not hasattr(self.conn_or_engine, "begin"):
                # Likely a Connection
                return self.conn_or_engine  # type: ignore[return-value]
            # Engine -> begin a connection
            return self.conn_or_engine.connect()  # type: ignore[return-value]

        def fetch_batch(self, limit: int = 100) -> list[OutboxEvent]:
            stmt = (
                select(self.table.c.id, self.table.c.topic, self.table.c.payload)
                .where(self.table.c.published_at.is_(None))
                .order_by(self.table.c.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            # Ensure transactional semantics for SKIP LOCKED
            results: list[OutboxEvent] = []
            conn = self._conn()
            close_conn = not hasattr(self.conn_or_engine, "execute") or hasattr(self.conn_or_engine, "begin")
            trans = None
            try:
                if hasattr(conn, "begin"):
                    trans = conn.begin()
                for row in conn.execute(stmt):
                    results.append(OutboxEvent(id=row[0], topic=row[1], payload=row[2]))
                if trans is not None:
                    trans.commit()
            finally:
                if trans is not None and trans.is_active:
                    trans.rollback()
                if close_conn:
                    conn.close()
            return results

        def mark_published(self, ids: list[int]) -> None:
            if not ids:
                return
            stmt = (
                update(self.table)
                .where(self.table.c.id.in_(ids))
                .values(published_at=func.now())
            )
            conn = self._conn()
            close_conn = not hasattr(self.conn_or_engine, "execute") or hasattr(self.conn_or_engine, "begin")
            trans = None
            try:
                if hasattr(conn, "begin"):
                    trans = conn.begin()
                conn.execute(stmt)
                if trans is not None:
                    trans.commit()
            finally:
                if trans is not None and trans.is_active:
                    trans.rollback()
                if close_conn:
                    conn.close()
except Exception:
    # SQLAlchemy not installed; leave integration unavailable
    pass
