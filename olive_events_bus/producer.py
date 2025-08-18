from __future__ import annotations

import json
import logging
from typing import Any

import backoff
from aiokafka import AIOKafkaProducer  # pyright: ignore[reportMissingTypeStubs]

from .config import SDKConfig
from .envelope import EventEnvelope
from .observability import metrics
from .schema import SchemaRegistry

logger = logging.getLogger(__name__)


class KafkaProducerClient:
    def __init__(self, cfg: SDKConfig, schema_registry: SchemaRegistry):
        self.cfg = cfg
        self.sr = schema_registry
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        if self._producer is not None:
            return
        kwargs: dict[str, Any] = {
            'bootstrap_servers': self.cfg.bootstrap_servers,
            'acks': self.cfg.acks,
            'linger_ms': self.cfg.linger_ms,
            'compression_type': 'gzip',
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
        self._producer = AIOKafkaProducer(**kwargs)
        await self._producer.start()
        logger.info('Kafka producer started', extra={'service': self.cfg.service_name})

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None
            logger.info('Kafka producer stopped', extra={'service': self.cfg.service_name})

    @backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=backoff.full_jitter)
    async def publish(
        self,
        *,
        domain: str,
        entity: str,
        event: str,
        version: str,
        payload: dict[str, Any],
        correlation_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> EventEnvelope:
        if not self._producer:
            await self.start()
        self.sr.validate(domain, f'{entity}.{event}', version, payload)

        envelope = EventEnvelope.build(
            event_type=f'{domain}.{entity}.{event}',
            event_version=version,
            source_service=self.cfg.service_name,
            payload=payload,
            correlation_id=correlation_id,
            extra_headers=headers,
        )
        topic = self.sr.topic(domain, entity, event, version)
        data = json.dumps(envelope.to_dict()).encode('utf-8')
        hdrs = [(k, v.encode('utf-8')) for k, v in (headers or {}).items()]
        await self._producer.send_and_wait(topic, data, headers=hdrs)
        metrics.events_published.labels(topic=topic, service=self.cfg.service_name, version=version).inc()
        logger.info(
            'event_published',
            extra={
                'topic': topic,
                'event_id': envelope.event_id,
                'event_type': envelope.event_type,
                'version': version,
                'correlation_id': envelope.correlation_id,
            },
        )
        return envelope
