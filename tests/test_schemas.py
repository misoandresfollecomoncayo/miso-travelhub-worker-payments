"""Tests for the PaymentWebhookPayload validator (REFUNDED vs. rest)."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.schemas.payment_webhook import PaymentWebhookPayload, PaymentWebhookStatus


def test_refunded_accepts_minimal_fields() -> None:
    payload = PaymentWebhookPayload(
        status=PaymentWebhookStatus.REFUNDED,
        message="Reembolso emitido",
        invoiceId="INV-9",
    )
    assert payload.transactionId is None
    assert payload.amount is None
    assert payload.currency is None
    assert payload.processedAt is None


def test_refunded_passes_through_optional_fields() -> None:
    payload = PaymentWebhookPayload(
        status=PaymentWebhookStatus.REFUNDED,
        message="Reembolso parcial",
        invoiceId="INV-9",
        amount=Decimal("50.00"),
        currency="COP",
        cardHolder="JOHN",
        maskedCard="**** **** **** 1234",
        transactionId="TX-9",
        processedAt=datetime(2026, 5, 2, 18, 13, 14, tzinfo=timezone.utc),
    )
    assert payload.transactionId == "TX-9"
    assert payload.amount == Decimal("50.00")


def test_approved_rejects_missing_optional_fields() -> None:
    with pytest.raises(ValidationError) as exc_info:
        PaymentWebhookPayload(
            status=PaymentWebhookStatus.APPROVED,
            message="ok",
            invoiceId="INV-1",
        )
    # The error should name every missing field.
    error_text = str(exc_info.value)
    for field in (
        "amount",
        "currency",
        "cardHolder",
        "maskedCard",
        "transactionId",
        "processedAt",
    ):
        assert field in error_text


def test_approved_with_full_payload_passes() -> None:
    payload = PaymentWebhookPayload(
        status=PaymentWebhookStatus.APPROVED,
        message="ok",
        invoiceId="INV-1",
        amount=Decimal("100"),
        currency="USD",
        cardHolder="JOHN",
        maskedCard="**** **** **** 1234",
        transactionId="TX-1",
        processedAt=datetime(2026, 5, 2, 18, 13, 14, tzinfo=timezone.utc),
    )
    assert payload.status == PaymentWebhookStatus.APPROVED
    assert payload.amount == Decimal("100")
