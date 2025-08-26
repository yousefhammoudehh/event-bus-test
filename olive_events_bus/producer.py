import json
import logging
from dataclasses import replace
from typing import Any, Unpack

import backoff
from aiokafka import AIOKafkaProducer

from .config import Config, ConfigOverrides
from .envelope import EventEnvelope
from .events import OliveEvent
from .observability import metrics
from .schema import schema_registry

logger = logging.getLogger(__name__)


class Producer:
    def __init__(self, cfg: Config | None = None, **overrides: Unpack[ConfigOverrides]):
        """Create a Producer.

        Args:
            cfg: Optional Config instance. If not provided, defaults from env are used.
            **overrides: Individual Config field overrides (e.g., acks='all', linger_ms=5).
        """
        # Base config from provided cfg or environment defaults, then apply overrides
        base_cfg = cfg or Config()
        # dataclasses.replace validates field names and immutably returns a new instance
        self.cfg = replace(base_cfg, **overrides) if overrides else base_cfg
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
    async def _publish_event_envelope(
        self, event_envelope: EventEnvelope, domain: str, entity: str, event: str
    ) -> None:
        if not self._producer:
            await self.start()

        if self._producer is None:
            # Should not happen as we start above, but guard for type-checkers
            raise RuntimeError('Producer not started')

        topic = schema_registry.topic(domain, entity, event)
        data = json.dumps(event_envelope.to_dict()).encode('utf-8')
        hdrs = [(k, v.encode('utf-8')) for k, v in (event_envelope.headers or {}).items()]

        await self._producer.send_and_wait(topic, data, headers=hdrs)
        metrics.events_published.labels(
            topic=topic, service=self.cfg.service_name, version=event_envelope.event_version
        ).inc()
        logger.info(
            'event_published',
            extra={
                'topic': topic,
                'event_id': event_envelope.event_id,
                'event_type': event_envelope.event_type,
                'version': event_envelope.event_version,
                'correlation_id': event_envelope.correlation_id,
            },
        )

    async def publish(
        self,
        *,
        domain: str,
        entity: str,
        event: str,
        payload: dict[str, Any],
        version: str | None = None,
        correlation_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> EventEnvelope:
        # Resolve version if not provided (latest) and validate
        resolved_version = version or await schema_registry.latest_version(domain, entity, event)
        await schema_registry.validate(domain, entity, event, resolved_version, payload)

        envelope = EventEnvelope.build(
            event_type=f'{domain}.{entity}.{event}',
            event_version=resolved_version,
            source_service=self.cfg.service_name,
            payload=payload,
            correlation_id=correlation_id,
            headers=headers,
        )
        await self._publish_event_envelope(envelope, domain, entity, event)

        return envelope

    @backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=backoff.full_jitter)
    async def publish_event(self, olive_event: OliveEvent) -> EventEnvelope:
        envelope = olive_event.to_envelope(self.cfg.service_name)
        await self._publish_event_envelope(envelope, olive_event.domain, olive_event.entity, olive_event.event)
        return envelope
