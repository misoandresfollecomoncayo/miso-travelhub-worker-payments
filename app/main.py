import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_router
from app.core.config import get_settings
from app.db.session import get_sessionmaker
from app.services.kafka_consumer import KafkaPaymentConsumer
from app.services.payment_event_handler import build_payment_event_handler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _log_database_config() -> None:
    s = get_settings()
    if not s.database_url:
        logger.warning(
            "DATABASE_URL is not set. Consumer will fail to persist messages."
        )
        return
    scheme, _, rest = s.database_url.partition("://")
    host_part = rest.split("@", 1)[-1] if "@" in rest else rest
    logger.info("Database configured: %s://***@%s", scheme or "?", host_part)


def _log_kafka_config() -> None:
    s = get_settings()
    if not s.kafka_enabled:
        logger.warning(
            "Kafka consumer DISABLED (KAFKA_ENABLED=false). "
            "No messages will be processed."
        )
        return

    missing = [
        name
        for name, value in (
            ("KAFKA_BOOTSTRAP_SERVERS", s.kafka_bootstrap_servers),
            ("KAFKA_TOPIC", s.kafka_topic),
            ("KAFKA_GROUP_ID", s.kafka_group_id),
        )
        if not value
    ]
    if missing:
        logger.error(
            "Kafka ENABLED but misconfigured. Missing: %s", ", ".join(missing)
        )
        return

    logger.info(
        "Kafka consumer ENABLED: bootstrap=%s topic=%s group=%s protocol=%s",
        s.kafka_bootstrap_servers,
        s.kafka_topic,
        s.kafka_group_id,
        s.kafka_security_protocol,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    session_factory = get_sessionmaker() if settings.database_url else None
    handler = (
        build_payment_event_handler(session_factory)
        if session_factory is not None
        else _no_db_handler
    )
    consumer = KafkaPaymentConsumer(settings, handler)
    app.state.kafka_consumer = consumer

    try:
        await consumer.start()
    except Exception:
        # Don't kill the container — /health stays up, /health/consumer reports 503.
        logger.exception("Kafka consumer failed to start; /health/consumer will 503")

    consumer.spawn()
    try:
        yield
    finally:
        await consumer.stop()


async def _no_db_handler(payload) -> None:  # noqa: ANN001
    raise RuntimeError(
        "DATABASE_URL not configured; cannot persist payment event"
    )


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        debug=settings.app_debug,
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_router, prefix="/api/v1")

    _log_database_config()
    _log_kafka_config()

    return app


app = create_app()
