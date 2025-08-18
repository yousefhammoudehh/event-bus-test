import logging

from .config import SDKConfig
from .envelope import EventEnvelope
from .schema import SchemaRegistry

# Optional exports; available if dependencies installed
try:  # pragma: no cover - optional
    from .consumer import KafkaConsumerClient
    from .producer import KafkaProducerClient

except Exception as exc:  # pragma: no cover - optional
    logging.exception('Failed to import optional Kafka dependencies: %s', exc)
    pass

__all__ = ['EventEnvelope', 'KafkaConsumerClient', 'KafkaProducerClient', 'SDKConfig', 'SchemaRegistry']
