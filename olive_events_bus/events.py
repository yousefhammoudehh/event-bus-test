import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from .envelope import EventEnvelope
from .schema import schema_registry


class OliveEventType(str, Enum):
    """Standard event types in the Olive ecosystem."""

    # Employee events
    EMPLOYEE_CREATED = 'employees.employee.created'
    EMPLOYEE_UPDATED = 'employees.employee.updated'
    EMPLOYEE_DELETED = 'employees.employee.deleted'
    EMPLOYEE_ACTIVATED = 'employees.employee.activated'
    EMPLOYEE_DEACTIVATED = 'employees.employee.deactivated'

    # Leave events (canonical dot notation)
    LEAVE_REQUESTED = 'leaves.leave.requested'
    LEAVE_UPDATED = 'leaves.leave.updated'
    LEAVE_APPROVED = 'leaves.leave.approved'
    LEAVE_REJECTED = 'leaves.leave.rejected'
    LEAVE_CANCELLED = 'leaves.leave.cancelled'

    @property
    def domain(self) -> str:
        return self.value.split('.')[0]

    @property
    def entity(self) -> str:
        return self.value.split('.')[1]

    @property
    def event(self) -> str:
        return self.value.split('.')[2]


@dataclass
class OliveEvent:
    """
    Standard event structure for the Olive ecosystem.

    This is the core event class that all events in the Olive platform
    should use.
    """

    event_type: OliveEventType
    payload: dict[str, Any]
    version: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    headers: dict[str, str] | None = None
    correlation_id: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def domain(self) -> str:
        """Domain part extracted from event_type (e.g., 'employees')."""
        return self.event_type.value.split('.')[0]

    @property
    def entity(self) -> str:
        """Entity part extracted from event_type (e.g., 'employee')."""
        return self.event_type.value.split('.')[1]

    @property
    def event(self) -> str:
        """Event name extracted from event_type (e.g., 'created')."""
        return self.event_type.value.split('.')[2]

    def to_envelope(self, service_name: str) -> EventEnvelope:
        return EventEnvelope.build(
            event_type=f'{self.domain}.{self.entity}.{self.event}',
            event_version=self.version,
            source_service=service_name,
            payload=self.payload,
            event_id=self.event_id,
            correlation_id=self.correlation_id,
            headers=self.headers,
        )

    @classmethod
    async def create(
        cls,
        event_type: OliveEventType,
        payload: dict[str, Any],
        *,
        version: str | None = None,
        event_id: str | None = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        init_correlation_id: bool = False,
    ) -> 'OliveEvent':
        if not correlation_id and init_correlation_id:
            correlation_id = str(uuid.uuid4())

        domain, entity, event = event_type.domain, event_type.entity, event_type.event
        if not version:
            version = await schema_registry.latest_version(domain, entity, event)
        await schema_registry.validate(domain, entity, event, version, payload)

        return cls(
            event_type=event_type,
            payload=payload,
            version=version,
            event_id=event_id or str(uuid.uuid4()),
            headers=headers,
            correlation_id=correlation_id,
        )
