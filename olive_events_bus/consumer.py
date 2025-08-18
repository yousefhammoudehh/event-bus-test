from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import backoff
from aiokafka import AIOKafkaConsumer

from .config import SDKConfig
from .observability import metrics
from .schema import SchemaRegistry

logger = logging.getLogger(__name__)

EventHandler = Callable[[dict[str, Any]], Awaitable[None]]


class KafkaConsumerClient:
    def __init__(self, cfg: SDKConfig, schema_registry: SchemaRegistry, *, group: str):
        self.cfg = cfg
        self.sr = schema_registry
        self.group = f'{cfg.group_id_prefix}{group}'
        self._consumer: AIOKafkaConsumer | None = None
        self._handler: EventHandler | None = None
        self._dlq_topic_suffix = cfg.dlq_suffix

    def _dlq_topic(self, topic: str) -> str:
        return f'{topic}{self._dlq_topic_suffix}'

    async def start(self, topic: str, handler: EventHandler) -> None:
        if self._consumer is not None:
            return
        self._handler = handler
        kwargs: dict[str, Any] = {
            'bootstrap_servers': self.cfg.bootstrap_servers,
            'group_id': self.group,
            'enable_auto_commit': self.cfg.enable_auto_commit,
            'auto_offset_reset': self.cfg.auto_offset_reset,
        }
        if self.cfg.security_protocol != 'PLAINTEXT':
            kwargs['security_protocol'] = self.cfg.security_protocol
        if self.cfg.sasl_mechanism:
            kwargs['sasl_mechanism'] = self.cfg.sasl_mechanism
            kwargs['sasl_plain_username'] = self.cfg.sasl_username
            kwargs['sasl_plain_password'] = self.cfg.sasl_password
        if self.cfg.ssl_cafile:
            kwargs['ssl_cafile'] = self.cfg.ssl_cafile
        if self.cfg.ssl_certfile:
            kwargs['ssl_certfile'] = self.cfg.ssl_certfile
        if self.cfg.ssl_keyfile:
            kwargs['ssl_keyfile'] = self.cfg.ssl_keyfile

        self._consumer = AIOKafkaConsumer(topic, **kwargs)
        await self._consumer.start()
        logger.info(
            'Kafka consumer started', extra={'service': self.cfg.service_name, 'group': self.group, 'topic': topic}
        )

    async def stop(self) -> None:
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
            logger.info('Kafka consumer stopped', extra={'service': self.cfg.service_name, 'group': self.group})

    async def _send_to_dlq(self, topic: str, data: bytes, headers: list[tuple[str, bytes]] | None) -> None:
        # For simplicity, use AIOKafkaConsumer's underlying client to send to DLQ via temporary producer
        from aiokafka import AIOKafkaProducer

        prod = AIOKafkaProducer(bootstrap_servers=self.cfg.bootstrap_servers)
        await prod.start()
        try:
            await prod.send_and_wait(self._dlq_topic(topic), data, headers=headers)
        finally:
            await prod.stop()

    @backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=backoff.full_jitter)
    async def _handle(self, msg_topic: str, msg_value: bytes, headers: list[tuple[str, bytes]] | None) -> None:
        assert self._handler is not None
        payload = json.loads(msg_value.decode('utf-8'))
        # Validate envelope shape and inner payload according to event_type and version
        event_type = payload.get('event_type')
        version = payload.get('event_version')
        if not event_type or not version:
            raise ValueError('Invalid event: missing type/version')
        # event_type format: domain.entity.event
        try:
            domain, entity, event = event_type.split('.')
        except ValueError:
            raise ValueError(f'Invalid event_type: {event_type}')
        self.sr.validate(domain, f'{entity}.{event}', version, payload.get('payload', {}))
        await self._handler(payload)

    async def poll_forever(self) -> None:
        if not self._consumer:
            raise RuntimeError('Consumer not started')
        assert self._consumer is not None
        async for msg in self._consumer:
            try:
                await self._handle(msg.topic, msg.value, msg.headers)
            except Exception:
                logger.exception('event_processing_failed')
                try:
                    await self._send_to_dlq(msg.topic, msg.value, msg.headers)
                    metrics.events_failed.labels(topic=msg.topic, service=self.cfg.service_name, group=self.group).inc()
                except Exception:
                    logger.exception('dlq_send_failed')
                # Continue loop; message stays committed only if enable_auto_commit
                continue
            else:
                metrics.events_consumed.labels(topic=msg.topic, service=self.cfg.service_name, group=self.group).inc()
