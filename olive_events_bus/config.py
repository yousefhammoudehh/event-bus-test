from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SDKConfig:
    """Configuration for the Olive Events Bus SDK.

    Values resolve from environment variables with sensible defaults for local dev.
    """

    bootstrap_servers: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    security_protocol: str = os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
    sasl_mechanism: str | None = os.getenv('KAFKA_SASL_MECHANISM')
    sasl_username: str | None = os.getenv('KAFKA_SASL_USERNAME')
    sasl_password: str | None = os.getenv('KAFKA_SASL_PASSWORD')
    ssl_cafile: str | None = os.getenv('KAFKA_SSL_CAFILE')
    ssl_certfile: str | None = os.getenv('KAFKA_SSL_CERTFILE')
    ssl_keyfile: str | None = os.getenv('KAFKA_SSL_KEYFILE')

    # Observability
    service_name: str = os.getenv('SERVICE_NAME', 'unknown-service')
    environment: str = os.getenv('ENVIRONMENT', 'development')

    # Producer/consumer tuning
    acks: str = os.getenv('KAFKA_ACKS', 'all')
    retries: int = int(os.getenv('KAFKA_RETRIES', '5'))
    linger_ms: int = int(os.getenv('KAFKA_LINGER_MS', '10'))
    batch_size: int = int(os.getenv('KAFKA_BATCH_SIZE', '16384'))

    # Consumer
    group_id_prefix: str = os.getenv('KAFKA_GROUP_PREFIX', 'olive-')
    enable_auto_commit: bool = os.getenv('KAFKA_ENABLE_AUTO_COMMIT', 'false').lower() == 'true'
    auto_offset_reset: str = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')

    # DLQ suffix
    dlq_suffix: str = os.getenv('KAFKA_DLQ_SUFFIX', '.dlq')

    # Schema registry (internal JSON-based registry)
    schema_dir: str = os.getenv('EVENT_SCHEMA_DIR', 'schemas')

    # Backoff policy
    max_retry_attempts: int = int(os.getenv('EVENT_MAX_RETRY_ATTEMPTS', '5'))
    initial_retry_backoff_ms: int = int(os.getenv('EVENT_INITIAL_BACKOFF_MS', '250'))
