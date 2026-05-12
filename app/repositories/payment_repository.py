import logging

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Payment
from app.db.session import get_db_session
from app.schemas.payment_webhook import PaymentWebhookPayload

logger = logging.getLogger(__name__)


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_from_webhook(
        self, payload: PaymentWebhookPayload
    ) -> Payment | None:
        existing = await self._session.scalar(
            select(Payment).where(Payment.transaction_id == payload.transactionId)
        )
        if existing is not None:
            logger.info(
                "payment already persisted tx=%s id=%s",
                payload.transactionId,
                existing.id,
            )
            return None

        payment = Payment(
            status=payload.status.value,
            message=payload.message,
            invoice_id=payload.invoiceId,
            amount=payload.amount,
            currency=payload.currency,
            card_holder=payload.cardHolder,
            masked_card=payload.maskedCard,
            transaction_id=payload.transactionId,
            processed_at=payload.processedAt,
        )
        self._session.add(payment)
        await self._session.commit()
        await self._session.refresh(payment)
        return payment


async def get_payment_repository(
    session: AsyncSession = Depends(get_db_session),
) -> PaymentRepository:
    return PaymentRepository(session)
