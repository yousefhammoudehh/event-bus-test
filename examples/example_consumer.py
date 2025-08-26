import importlib
import logging

from olive_events_bus import Config, Consumer, fastapi_lifespan

logger = logging.getLogger(__name__)


async def handle_leave_requested(envelope):
    logger.info("leave requested", extra={"payload": envelope.payload})


async def handle_leave_approved(envelope):
    logger.info("leave approved", extra={"payload": envelope.payload})


# Build a single consumer and register multiple handlers
consumer = (
    Consumer(Config(service_name="olive-notifications"), group="notifications-consumer")
    .on("leaves.leave.requested", handle_leave_requested)
    .on("leaves.leave.approved", handle_leave_approved)
)

FastAPI = importlib.import_module("fastapi").FastAPI  # type: ignore[attr-defined]
# Plug into FastAPI lifespan for non-blocking startup/shutdown
app = FastAPI(title="Example SDK Consumer")
app.router.lifespan_context = fastapi_lifespan(consumer)  # type: ignore[attr-defined]


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}