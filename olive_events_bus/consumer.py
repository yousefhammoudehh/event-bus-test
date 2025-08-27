from __future__ import annotations

import asyncio
import inspect
import json
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import replace
from typing import Any, Unpack

import backoff
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from aiokafka.structs import OffsetAndMetadata, TopicPartition

from .config import Config, ConfigOverrides
from .envelope import EventEnvelope
from .events import OliveEvent, OliveEventType
from .observability import metrics

logger = logging.getLogger(__name__)

# Type for consumer handler callbacks
ConsumerHandler = Callable[[OliveEvent], Awaitable[None] | None]


class Consumer:
    def __init__(self, group: str | None = None, cfg: Config | None = None, **overrides: Unpack[ConfigOverrides]):
        base_cfg = cfg or Config()
        self.cfg = replace(base_cfg, **overrides) if overrides else base_cfg
        self.group = group or f'{self.cfg.service_name}-group'
        # Internal state
        self._consumer: AIOKafkaConsumer | None = None
        self._dlq_producer: AIOKafkaProducer | None = None
        self._handlers: dict[OliveEventType, ConsumerHandler] = {}
        self._default_handler: ConsumerHandler | None = None
        self._started: bool = False
        self._task: asyncio.Task[None] | None = None

    def on(self, event_type: OliveEventType, handler: ConsumerHandler) -> Consumer:
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
    async def start(self) -> None:
        if self._started:
            return

        sub_topics = [key.value if (isinstance(key, OliveEventType)) else key for key in self._handlers]
        if not sub_topics:
            raise ValueError('No topics to subscribe to. Register handlers via .on(...) or pass topics to start().')

        # Build consumer kwargs from config
        ckwargs: dict[str, Any] = {
            'bootstrap_servers': self.cfg.bootstrap_servers,
            'group_id': self.group,
            'enable_auto_commit': self.cfg.enable_auto_commit,
            'auto_offset_reset': self.cfg.auto_offset_reset,
            'value_deserializer': lambda v: v,  # raw bytes, we'll decode explicitly
        }
        if self.cfg.security_protocol != 'PLAINTEXT':
            ckwargs['security_protocol'] = self.cfg.security_protocol
        if self.cfg.sasl_mechanism:
            ckwargs['sasl_mechanism'] = self.cfg.sasl_mechanism
            ckwargs['sasl_plain_username'] = self.cfg.sasl_username
            ckwargs['sasl_plain_password'] = self.cfg.sasl_password
        if self.cfg.ssl_cafile:
            ckwargs['ssl_cafile'] = self.cfg.ssl_cafile
        if self.cfg.ssl_certfile:
            ckwargs['ssl_certfile'] = self.cfg.ssl_certfile
        if self.cfg.ssl_keyfile:
            ckwargs['ssl_keyfile'] = self.cfg.ssl_keyfile

        self._consumer = AIOKafkaConsumer(*sub_topics, **ckwargs)
        await self._consumer.start()
        logger.info('Kafka consumer started', extra={'service': self.cfg.service_name, 'group': self.group})

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
        logger.info('Kafka consumer stopped', extra={'service': self.cfg.service_name, 'group': self.group})

    async def run_forever(self) -> None:
        """Start (if needed) and consume indefinitely, processing each message."""
        if not self._started:
            # Derive topics from handlers by default
            await self.start()
        if self._consumer is None:
            raise RuntimeError('Consumer not started')

        try:
            async for msg in self._consumer:
                await self._handle_message(msg)
        except asyncio.CancelledError:  # graceful shutdown
            raise
        finally:
            await self.stop()

    # -------- Background helpers --------
    def start_background(self) -> asyncio.Task[None]:
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
    async def _handle_message(self, msg: ConsumerRecord[str, bytes]) -> None:
        topic = getattr(msg, 'topic', '<unknown>')
        partition = getattr(msg, 'partition', -1)
        offset = getattr(msg, 'offset', -1)

        # Decode JSON envelope
        payload_bytes: bytes = b''
        olive_event: OliveEvent | None = None
        try:
            payload_bytes = msg.value or b''
            data = json.loads(payload_bytes.decode('utf-8'))
            envelope = EventEnvelope(**data)
            olive_event = await OliveEvent.from_envelope(envelope)  # validate conversion
        except Exception as exc:  # malformed payload -> DLQ
            logger.exception('Failed to decode message as EventEnvelope', extra={'topic': topic, 'offset': offset})
            await self._send_to_dlq(topic, payload_bytes, error=exc)
            await self._commit_offset(topic, partition, offset)
            metrics.events_failed.labels(topic=topic, service=self.cfg.service_name, group=self.group).inc()
            return

        # Process with backoff retry
        try:
            await self._process_event_with_retry(olive_event, topic)
            # Success path
            await self._commit_offset(topic, partition, offset)
            metrics.events_consumed.labels(topic=topic, service=self.cfg.service_name, group=self.group).inc()
        except Exception as exc:
            # All retries exhausted -> DLQ
            await self._fail_with_dlq(topic, envelope, exc, partition, offset)

    async def _process_event_with_retry(self, olive_event: OliveEvent, topic: str) -> None:
        """Process an event with backoff retry logic."""

        @backoff.on_exception(
            backoff.expo,
            Exception,
            max_tries=max(1, self.cfg.max_retry_attempts),
            base=self.cfg.initial_retry_backoff_ms / 1000.0,
            jitter=backoff.full_jitter,
        )
        async def _process() -> None:
            # Dispatch
            handler = self._handlers.get(olive_event.event_type, self._default_handler)
            if handler is None:
                logger.warning(
                    'No handler registered for event_type; dropping',
                    extra={'event_type': olive_event.event_type, 'topic': topic},
                )
            else:
                res = handler(olive_event)
                if inspect.isawaitable(res):
                    await res

        await _process()

    async def _fail_with_dlq(
        self, topic: str, envelope: EventEnvelope, exc: Exception, partition: int, offset: int
    ) -> None:
        logger.exception(
            'Event processing failed; sending to DLQ',
            extra={
                'topic': topic,
                'event_id': envelope.event_id,
                'event_type': envelope.event_type,
                'version': envelope.event_version,
                'offset': offset,
            },
        )
        await self._send_to_dlq(topic, json.dumps(envelope.to_dict()).encode('utf-8'), error=exc)
        await self._commit_offset(topic, partition, offset)
        metrics.events_failed.labels(topic=topic, service=self.cfg.service_name, group=self.group).inc()

    async def _commit_offset(self, topic: str, partition: int, offset: int) -> None:
        # Only commit manually when auto-commit is disabled
        if not self._consumer or self.cfg.enable_auto_commit:
            return
        try:
            tp = TopicPartition(topic, partition)
            await self._consumer.commit({tp: OffsetAndMetadata(offset + 1, '')})
        except Exception:
            logger.exception(
                'Failed to commit offset', extra={'topic': topic, 'partition': partition, 'offset': offset}
            )

    async def _ensure_dlq_producer(self) -> None:
        if self._dlq_producer is not None:
            return
        # Reuse config for producer; keep minimal options
        pkwargs: dict[str, Any] = {
            'bootstrap_servers': self.cfg.bootstrap_servers,
            'compression_type': 'gzip',
            'linger_ms': self.cfg.linger_ms,
            'acks': self.cfg.acks,
        }
        if self.cfg.security_protocol != 'PLAINTEXT':
            pkwargs['security_protocol'] = self.cfg.security_protocol
        if self.cfg.sasl_mechanism:
            pkwargs['sasl_mechanism'] = self.cfg.sasl_mechanism
            pkwargs['sasl_plain_username'] = self.cfg.sasl_username
            pkwargs['sasl_plain_password'] = self.cfg.sasl_password
        if self.cfg.ssl_cafile:
            pkwargs['ssl_cafile'] = self.cfg.ssl_cafile
        if self.cfg.ssl_certfile:
            pkwargs['ssl_certfile'] = self.cfg.ssl_certfile
        if self.cfg.ssl_keyfile:
            pkwargs['ssl_keyfile'] = self.cfg.ssl_keyfile

        self._dlq_producer = AIOKafkaProducer(**pkwargs)
        await self._dlq_producer.start()

    async def _send_to_dlq(self, topic: str, value: bytes, *, error: Exception) -> None:
        dlq_topic = f'{topic}{self.cfg.dlq_suffix}'
        await self._ensure_dlq_producer()
        if self._dlq_producer is None:
            raise RuntimeError('DLQ producer not started')

        # Attach minimal error metadata via headers to keep payload unchanged
        hdrs = [
            ('x-error-type', type(error).__name__.encode('utf-8')),
            ('x-error-msg', str(error).encode('utf-8')),
            ('x-source-service', self.cfg.service_name.encode('utf-8')),
            ('x-consumer-group', self.group.encode('utf-8')),
        ]
        try:
            await self._dlq_producer.send_and_wait(dlq_topic, value, headers=hdrs)
        except Exception:
            logger.exception('Failed to publish to DLQ', extra={'dlq_topic': dlq_topic})
