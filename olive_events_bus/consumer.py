from __future__ import annotations

import asyncio
import inspect
import json
import logging
from collections.abc import Awaitable, Callable, Iterable
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from typing import Any, Unpack

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import OffsetAndMetadata, TopicPartition

from .config import Config, ConfigOverrides
from .envelope import EventEnvelope
from .observability import metrics
from .schema import schema_registry

logger = logging.getLogger(__name__)

# Type for consumer handler callbacks
ConsumerHandler = Callable[[EventEnvelope], Awaitable[None] | None]


class Consumer:
    """Kafka consumer with minimal service-side setup.

    Key goals:
    - Mirror Producer patterns (config overrides, start/stop lifecycle, observability).
    - Auto-subscribe to topics based on registered event handlers.
    - Validate payloads against JSON Schemas before invoking handlers.
    - Retry with backoff, then DLQ on failure, with offset commit policy configurable.

    Typical usage:
        consumer = Consumer(group="leaves-service") \
            .on("leaves.leave.requested", handle_leave_requested)
        await consumer.run_forever()
    """

    def __init__(self, cfg: Config | None = None, *, group: str | None = None, **overrides: Unpack[ConfigOverrides]):
        base_cfg = cfg or Config()
        self.cfg = replace(base_cfg, **overrides) if overrides else base_cfg
        self.group = group or f"{self.cfg.service_name}-group"
        # Internal state
        self._consumer = None
        self._dlq_producer = None
        self._handlers = {}
        self._default_handler = None
        self._started = False
        self._task = None

    # -------- Registration API --------
    def on(self, event_type: str, handler: ConsumerHandler) -> Consumer:
        """Register a handler for a specific event type (domain.entity.event).

        Accepts plain strings like "leaves.leave.requested" or Enum.value.
        Returns self for a fluent API.
        """
        self._handlers[event_type] = handler
        return self

    def on_default(self, handler: ConsumerHandler) -> Consumer:
        """Register a fallback handler for unrecognized event types."""
        self._default_handler = handler
        return self

    # -------- Lifecycle --------
    async def start(self, topics: Iterable[str] | None = None) -> None:
        if self._started:
            return

        # Resolve subscription topics from registered handlers if not provided
        sub_topics = list(topics) if topics is not None else list(self._handlers)
        if not sub_topics:
            raise ValueError(
                "No topics to subscribe to. Register handlers via .on(...) or pass topics to start()."
            )

        # Build consumer kwargs from config
        ckwargs: dict[str, Any] = {
            "bootstrap_servers": self.cfg.bootstrap_servers,
            "group_id": self.group,
            "enable_auto_commit": self.cfg.enable_auto_commit,
            "auto_offset_reset": self.cfg.auto_offset_reset,
            "value_deserializer": lambda v: v,  # raw bytes, we'll decode explicitly
        }
        if self.cfg.security_protocol != "PLAINTEXT":
            ckwargs["security_protocol"] = self.cfg.security_protocol
        if self.cfg.sasl_mechanism:
            ckwargs["sasl_mechanism"] = self.cfg.sasl_mechanism
            ckwargs["sasl_plain_username"] = self.cfg.sasl_username
            ckwargs["sasl_plain_password"] = self.cfg.sasl_password
        if self.cfg.ssl_cafile:
            ckwargs["ssl_cafile"] = self.cfg.ssl_cafile
        if self.cfg.ssl_certfile:
            ckwargs["ssl_certfile"] = self.cfg.ssl_certfile
        if self.cfg.ssl_keyfile:
            ckwargs["ssl_keyfile"] = self.cfg.ssl_keyfile

        self._consumer = AIOKafkaConsumer(*sub_topics, **ckwargs)
        await self._consumer.start()
        logger.info("Kafka consumer started", extra={"service": self.cfg.service_name, "group": self.group})

        # DLQ producer (lazy-start; we start on first use to keep startup fast)
        self._dlq_producer = None
        self._started = True

    async def stop(self) -> None:
        tasks: list[Awaitable[Any]] = []
        if self._consumer is not None:
            tasks.append(self._consumer.stop())
        if self._dlq_producer is not None:
            tasks.append(self._dlq_producer.stop())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._consumer = None
        self._dlq_producer = None
        self._started = False
        logger.info("Kafka consumer stopped", extra={"service": self.cfg.service_name, "group": self.group})

    async def run_forever(self) -> None:
        """Start (if needed) and consume indefinitely, processing each message."""
        if not self._started:
            # Derive topics from handlers by default
            await self.start()
        if self._consumer is None:
            raise RuntimeError("Consumer not started")

        try:
            async for msg in self._consumer:
                await self._handle_message(msg)
        except asyncio.CancelledError:  # graceful shutdown
            raise
        finally:
            await self.stop()

    # -------- Background helpers --------
    def start_background(self) -> asyncio.Task:
        """Start the consumer loop in a background task.

        Returns the created asyncio.Task. Safe to call once; subsequent calls
        while a task is active will raise RuntimeError.
        """
        if self._task and not self._task.done():
            raise RuntimeError('Consumer background task already running')
        self._task = asyncio.create_task(self.run_forever())
        return self._task

    async def stop_background(self) -> None:
        """Cancel and await the background task, then ensure the consumer stops."""
        if self._task and not self._task.done():
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        self._task = None
        # Ensure underlying client is stopped (idempotent)
        await self.stop()

    # -------- Processing --------
    async def _handle_message(self, msg: Any) -> None:  # msg is aiokafka.structs.ConsumerRecord
        topic = getattr(msg, "topic", "<unknown>")
        partition = getattr(msg, "partition", -1)
        offset = getattr(msg, "offset", -1)

        # Decode JSON envelope
        try:
            payload_bytes: bytes = msg.value  # type: ignore[assignment]
            data = json.loads(payload_bytes.decode("utf-8"))
            envelope = EventEnvelope(**data)
        except Exception as exc:  # malformed payload -> DLQ
            logger.exception("Failed to decode message as EventEnvelope", extra={"topic": topic, "offset": offset})
            await self._send_to_dlq(topic, payload_bytes if "payload_bytes" in locals() else b"", error=exc)
            await self._commit_offset(topic, partition, offset)
            metrics.events_failed.labels(topic=topic, service=self.cfg.service_name, group=self.group).inc()
            return

        # Validate against schema and dispatch
        event_type = envelope.event_type
        try:
            domain, entity, event = event_type.split(".")
        except Exception:
            await self._fail_with_dlq(topic, envelope, ValueError(f"Invalid event_type: {event_type!r}"), partition, offset)
            return

        # Retry attempts before DLQ
        attempts = 0
        max_attempts = max(1, self.cfg.max_retry_attempts)
        backoff_ms = max(0, self.cfg.initial_retry_backoff_ms)

        while True:
            try:
                # Validate schema version declared by the producer
                await schema_registry.validate(domain, entity, event, envelope.event_version, envelope.payload)

                # Dispatch
                handler = self._handlers.get(event_type, self._default_handler)
                if handler is None:
                    logger.warning(
                        "No handler registered for event_type; dropping",
                        extra={"event_type": event_type, "topic": topic},
                    )
                else:
                    res = handler(envelope)
                    if inspect.isawaitable(res):
                        await res

                # Success path
                metrics.events_consumed.labels(topic=topic, service=self.cfg.service_name, group=self.group).inc()
                await self._commit_offset(topic, partition, offset)
                return

            except Exception as exc:  # validation or handler failure
                attempts += 1
                if attempts >= max_attempts:
                    await self._fail_with_dlq(topic, envelope, exc, partition, offset)
                    return
                # Transient failure -> backoff and retry
                sleep_for = (backoff_ms / 1000.0) * (2 ** (attempts - 1))
                await asyncio.sleep(sleep_for)

    async def _fail_with_dlq(
        self, topic: str, envelope: EventEnvelope, exc: Exception, partition: int, offset: int
    ) -> None:
        logger.exception(
            "Event processing failed; sending to DLQ",
            extra={
                "topic": topic,
                "event_id": envelope.event_id,
                "event_type": envelope.event_type,
                "version": envelope.event_version,
                "offset": offset,
            },
        )
        await self._send_to_dlq(topic, json.dumps(envelope.to_dict()).encode("utf-8"), error=exc)
        await self._commit_offset(topic, partition, offset)
        metrics.events_failed.labels(topic=topic, service=self.cfg.service_name, group=self.group).inc()

    async def _commit_offset(self, topic: str, partition: int, offset: int) -> None:
        # Only commit manually when auto-commit is disabled
        if not self._consumer or self.cfg.enable_auto_commit:
            return
        try:
            tp = TopicPartition(topic, partition)
            await self._consumer.commit({tp: OffsetAndMetadata(offset + 1, None)})
        except Exception:
            logger.exception("Failed to commit offset", extra={"topic": topic, "partition": partition, "offset": offset})

    async def _ensure_dlq_producer(self) -> None:
        if self._dlq_producer is not None:
            return
        # Reuse config for producer; keep minimal options
        pkwargs: dict[str, Any] = {
            "bootstrap_servers": self.cfg.bootstrap_servers,
            "compression_type": "gzip",
            "linger_ms": self.cfg.linger_ms,
            "acks": self.cfg.acks,
        }
        if self.cfg.security_protocol != "PLAINTEXT":
            pkwargs["security_protocol"] = self.cfg.security_protocol
        if self.cfg.sasl_mechanism:
            pkwargs["sasl_mechanism"] = self.cfg.sasl_mechanism
            pkwargs["sasl_plain_username"] = self.cfg.sasl_username
            pkwargs["sasl_plain_password"] = self.cfg.sasl_password
        if self.cfg.ssl_cafile:
            pkwargs["ssl_cafile"] = self.cfg.ssl_cafile
        if self.cfg.ssl_certfile:
            pkwargs["ssl_certfile"] = self.cfg.ssl_certfile
        if self.cfg.ssl_keyfile:
            pkwargs["ssl_keyfile"] = self.cfg.ssl_keyfile

        self._dlq_producer = AIOKafkaProducer(**pkwargs)
        await self._dlq_producer.start()

    async def _send_to_dlq(self, topic: str, value: bytes, *, error: Exception) -> None:
        dlq_topic = f"{topic}{self.cfg.dlq_suffix}"
        await self._ensure_dlq_producer()
        if self._dlq_producer is None:
            raise RuntimeError("DLQ producer not started")

        # Attach minimal error metadata via headers to keep payload unchanged
        hdrs = [
            ("x-error-type", type(error).__name__.encode("utf-8")),
            ("x-error-msg", str(error).encode("utf-8")),
            ("x-source-service", self.cfg.service_name.encode("utf-8")),
            ("x-consumer-group", self.group.encode("utf-8")),
        ]
        try:
            await self._dlq_producer.send_and_wait(dlq_topic, value, headers=hdrs)
        except Exception:
            logger.exception("Failed to publish to DLQ", extra={"dlq_topic": dlq_topic})


def fastapi_lifespan(consumer: Consumer):
    """Create a FastAPI lifespan callable that runs the consumer in the background.

    Usage:
        consumer = (
            Consumer(Config(service_name='svc'), group='group')
            .on('leaves.leave.requested', handler)
        )
        app = FastAPI(lifespan=fastapi_lifespan(consumer))
    """

    @asynccontextmanager
    async def _lifespan(_app):  # type: ignore[unused-argument]
        consumer.start_background()
        try:
            # Yield control so the background task can start
            await asyncio.sleep(0)
            yield
        finally:
            await consumer.stop_background()

    return _lifespan
