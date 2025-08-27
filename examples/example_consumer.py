import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from olive_events_bus import Consumer, OliveEvent, OliveEventType

logger = logging.getLogger(__name__)


async def handle_leave_requested(event: OliveEvent) -> None:
    logger.info('leave requested', extra={'payload': event.payload})


async def handle_leave_approved(event: OliveEvent) -> None:
    logger.info('leave approved', extra={'payload': event.payload})


# Build a single consumer and register multiple handlers
consumer = (
    Consumer(service_name='olive-notifications')
    .on(OliveEventType.LEAVE_REQUESTED, handle_leave_requested)
    .on(OliveEventType.LEAVE_APPROVED, handle_leave_approved)
)


@asynccontextmanager
async def lifespan(_app: Any) -> AsyncIterator[None]:
    # Startup: start consumer in background
    consumer.start_background()
    try:
        yield
    finally:
        # Shutdown: stop background consumer
        await consumer.stop_background()


# Plug into FastAPI lifespan for non-blocking startup/shutdown
app = FastAPI(title='Example SDK Consumer', lifespan=lifespan)  # type: ignore  # noqa: F821


@app.get('/health')  # type: ignore
async def health() -> dict[str, str]:
    return {'status': 'ok'}
