"""Bridge between the Kafka consumer and the SQL repository.

Builds the ``on_message`` callback that the ``KafkaPaymentConsumer`` calls
once per message. We keep it as a closure factory so the consumer stays
ignorant of SQLAlchemy.

After persisting an APPROVED payment we also POST a notification to the
notification-services Cloud Run service so the user gets notified. The
notification call is best-effort: a failure is logged but doesn't prevent
the Kafka offset from being committed — the booking is already paid in
the database, the source of truth.
"""

import logging
from typing import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.repositories.payment_repository import PaymentRepository
from app.schemas.payment_webhook import PaymentWebhookPayload, PaymentWebhookStatus
from app.services.notification_client import NotificationClient

logger = logging.getLogger(__name__)


def build_payment_event_handler(
    session_factory: async_sessionmaker[AsyncSession],
    notifier: NotificationClient | None = None,
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

        # Best-effort booking-paid notification. Only fires for APPROVED
        # events — and only when we actually persisted (skip duplicates so
        # the user is not notified twice on a re-delivery).
        if (
            notifier is not None
            and payment is not None
            and payload.status == PaymentWebhookStatus.APPROVED
        ):
            booking_id = payload.invoiceId
            try:
                ok = await notifier.notify_booking_paid(booking_id=booking_id)
            except Exception:
                # Defensive: the client already swallows httpx errors, but
                # we never want a notification bug to poison-pill the offset.
                logger.exception(
                    "notification raised unexpectedly booking_id=%s", booking_id
                )
                return
            if not ok:
                logger.warning(
                    "booking-paid notification failed booking_id=%s tx=%s",
                    booking_id,
                    payload.transactionId,
                )

    return _handler
