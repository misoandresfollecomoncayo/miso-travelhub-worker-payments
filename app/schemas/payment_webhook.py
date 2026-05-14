from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field, model_validator


class PaymentWebhookStatus(str, Enum):
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"
    PENDING = "PENDING"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"


class PaymentWebhookPayload(BaseModel):
    """Payment event payload.

    Field requirements depend on ``status``:

    - For ``REFUNDED`` only ``status``, ``message`` and ``invoiceId`` are
      required; the rest of the fields become optional because a refund
      event may be issued out-of-band without rebroadcasting the original
      transaction data.
    - For every other status the full payload is still required.
    """

    status: PaymentWebhookStatus
    message: str = Field(..., max_length=512)
    invoiceId: str = Field(..., min_length=1, max_length=64)

    # Required for everything except REFUNDED — enforced by the model validator.
    amount: Decimal | None = Field(default=None, ge=0)
    currency: str | None = Field(default=None, min_length=3, max_length=3)
    cardHolder: str | None = Field(default=None, min_length=1, max_length=128)
    maskedCard: str | None = Field(default=None, min_length=1, max_length=32)
    transactionId: str | None = Field(default=None, min_length=1, max_length=64)
    processedAt: datetime | None = None

    _FIELDS_REQUIRED_OUTSIDE_REFUND: tuple[str, ...] = (
        "amount",
        "currency",
        "cardHolder",
        "maskedCard",
        "transactionId",
        "processedAt",
    )

    @model_validator(mode="after")
    def _enforce_fields_for_non_refunded(self) -> "PaymentWebhookPayload":
        if self.status == PaymentWebhookStatus.REFUNDED:
            return self
        missing = [
            name
            for name in self._FIELDS_REQUIRED_OUTSIDE_REFUND
            if getattr(self, name) is None
        ]
        if missing:
            raise ValueError(
                "fields required when status != REFUNDED: " + ", ".join(missing)
            )
        return self


class PaymentWebhookAck(BaseModel):
    received: bool = True
    # transactionId may be absent on REFUNDED events.
    transactionId: str | None = None
    status: PaymentWebhookStatus
