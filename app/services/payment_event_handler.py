"""Bridge between the Kafka consumer and the SQL repository.

Builds the ``on_message`` callback that the ``KafkaPaymentConsumer`` calls
once per message. We keep it as a closure factory so the consumer stays
ignorant of SQLAlchemy.

After persisting an APPROVED payment we fan out to two best-effort
notifications:

1. ``notifier.notify_booking_paid(booking_id)`` — POSTs to the legacy
   send-notification endpoint (push pipeline).
2. ``email_notifier.send_payment_completed(...)`` — POSTs a
   ``payment.completed`` event to the events endpoint (email pipeline).
   This one needs ``user_id``, which we resolve from the ``reserva``
   table (column ``viajeroId``).

Both calls are independent: if one fails, the other still attempts. A
notification failure never prevents the Kafka offset from being committed
— the booking is already paid in the payments table, the source of
truth.
"""

import logging
from typing import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.repositories.booking_repository import BookingRepository
from app.repositories.payment_repository import PaymentRepository
from app.schemas.payment_webhook import PaymentWebhookPayload, PaymentWebhookStatus
from app.services.email_notification_client import EmailNotificationClient
from app.services.notification_client import NotificationClient

logger = logging.getLogger(__name__)


def build_payment_event_handler(
    session_factory: async_sessionmaker[AsyncSession],
    notifier: NotificationClient | None = None,
    email_notifier: EmailNotificationClient | None = None,
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

        # Only fire notifications when we actually persisted a new APPROVED
        # payment. Duplicates (re-deliveries) and non-APPROVED statuses are
        # skipped so the user is not notified twice / for nothing.
        should_notify = (
            payment is not None
            and payload.status == PaymentWebhookStatus.APPROVED
        )
        if not should_notify:
            return

        booking_id = payload.invoiceId

        # --- Channel 1: booking-paid push notification ---------------------
        if notifier is not None:
            try:
                ok = await notifier.notify_booking_paid(booking_id=booking_id)
            except Exception:
                # Defensive: client already swallows httpx errors, but we
                # never want a notification bug to poison-pill the offset.
                logger.exception(
                    "notification raised unexpectedly booking_id=%s",
                    booking_id,
                )
            else:
                if not ok:
                    logger.warning(
                        "booking-paid notification failed booking_id=%s tx=%s",
                        booking_id,
                        payload.transactionId,
                    )

        # --- Channel 2: payment.completed email event ---------------------
        if email_notifier is not None:
            # Resolve user_id (viajeroId) in its own session — the payments
            # session above is already committed/closed.
            async with session_factory() as session:
                booking_repo = BookingRepository(session)
                try:
                    user_id = await booking_repo.get_viajero_id(booking_id)
                except Exception:
                    logger.exception(
                        "viajeroId lookup failed booking_id=%s; "
                        "skipping email notification",
                        booking_id,
                    )
                    return

            if not user_id:
                # Booking not found, or column is null — nothing to do.
                return

            try:
                ok = await email_notifier.send_payment_completed(
                    user_id=user_id,
                    payment_id=payload.transactionId or "",
                    booking_id=booking_id,
                    amount=payload.amount if payload.amount is not None else 0,
                    currency=payload.currency or "",
                )
            except Exception:
                logger.exception(
                    "email notification raised unexpectedly booking_id=%s",
                    booking_id,
                )
            else:
                if not ok:
                    logger.warning(
                        "payment.completed email failed booking_id=%s "
                        "payment_id=%s",
                        booking_id,
                        payload.transactionId,
                    )

    return _handler
