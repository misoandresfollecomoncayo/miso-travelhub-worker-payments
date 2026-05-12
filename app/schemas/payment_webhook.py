from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field


class PaymentWebhookStatus(str, Enum):
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"
    PENDING = "PENDING"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"


class PaymentWebhookPayload(BaseModel):
    status: PaymentWebhookStatus
    message: str = Field(..., max_length=512)
    invoiceId: str = Field(..., min_length=1, max_length=64)
    amount: Decimal = Field(..., ge=0)
    currency: str = Field(..., min_length=3, max_length=3)
    cardHolder: str = Field(..., min_length=1, max_length=128)
    maskedCard: str = Field(..., min_length=1, max_length=32)
    transactionId: str = Field(..., min_length=1, max_length=64)
    processedAt: datetime


class PaymentWebhookAck(BaseModel):
    received: bool = True
    transactionId: str
    status: PaymentWebhookStatus
