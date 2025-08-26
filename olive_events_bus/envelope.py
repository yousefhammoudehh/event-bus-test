import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any


def iso_utc_ts() -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def _empty_str_dict() -> dict[str, str]:
    return {}


@dataclass
class EventEnvelope:
    event_id: str
    event_type: str
    event_version: str
    timestamp: str
    source_service: str
    payload: dict[str, Any]
    correlation_id: str | None = None
    headers: dict[str, str] = field(default_factory=_empty_str_dict)

    @classmethod
    def build(
        cls,
        *,
        event_type: str,
        event_version: str,
        source_service: str,
        payload: dict[str, Any],
        event_id: str | None = None,
        correlation_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> 'EventEnvelope':
        return cls(
            event_id=event_id or str(uuid.uuid4()),
            event_type=event_type,
            event_version=event_version,
            timestamp=iso_utc_ts(),
            source_service=source_service,
            payload=payload,
            correlation_id=correlation_id,
            headers=headers or {},
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
