"""Bridge between the Kafka consumer and the SQL repository.

Builds the ``on_message`` callback that the ``KafkaPaymentConsumer`` calls
once per message. We keep it as a closure factory so the consumer stays
ignorant of SQLAlchemy.
"""

import logging
from typing import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.repositories.payment_repository import PaymentRepository
from app.schemas.payment_webhook import PaymentWebhookPayload

logger = logging.getLogger(__name__)


def build_payment_event_handler(
    session_factory: async_sessionmaker[AsyncSession],
) -> Callable[[PaymentWebhookPayload], Awaitable[None]]:
    async def _handler(payload: PaymentWebhookPayload) -> None:
        async with session_factory() as session:
            repo = PaymentRepository(session)
            payment = await repo.create_from_webhook(payload)
        if payment is None:
            logger.info(
                "payment-event duplicate (already persisted) tx=%s",
                payload.transactionId,
            )
        else:
            logger.info(
                "payment-event persisted id=%s tx=%s status=%s",
                payment.id,
                payload.transactionId,
                payload.status.value,
            )

    return _handler
