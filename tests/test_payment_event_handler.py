"""Unit tests for build_payment_event_handler.

The handler glues the Kafka consumer to PaymentRepository — we inject a fake
session factory and a fake repository class so the test never touches a DB.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from app.schemas.payment_webhook import PaymentWebhookPayload, PaymentWebhookStatus
from app.services import payment_event_handler as handler_module


def _payload() -> PaymentWebhookPayload:
    return PaymentWebhookPayload(
        status=PaymentWebhookStatus.APPROVED,
        message="ok",
        invoiceId="INV-1",
        amount=Decimal("123.45"),
        currency="COP",
        cardHolder="JOHN",
        maskedCard="**** **** **** 1234",
        transactionId="TX-1",
        processedAt=datetime(2026, 5, 2, 18, 13, 14, tzinfo=timezone.utc),
    )


class FakeSession:
    def __init__(self) -> None:
        self.closed = False

    async def __aenter__(self) -> "FakeSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.closed = True


class FakeSessionFactory:
    def __init__(self) -> None:
        self.sessions: list[FakeSession] = []

    def __call__(self) -> FakeSession:
        s = FakeSession()
        self.sessions.append(s)
        return s


class FakeRepository:
    def __init__(self, session) -> None:
        self.session = session
        self.calls: list[PaymentWebhookPayload] = []

    async def create_from_webhook(
        self, payload: PaymentWebhookPayload
    ) -> Any:
        self.calls.append(payload)
        # Mimic the real repo's contract: returns a Payment-like or None.
        return type("FakePayment", (), {"id": 42})()


class FakeDuplicateRepository(FakeRepository):
    async def create_from_webhook(
        self, payload: PaymentWebhookPayload
    ) -> Any:
        self.calls.append(payload)
        return None  # duplicate path


@pytest.fixture
def patch_repo(monkeypatch):
    instances: list[FakeRepository] = []

    def factory(session):
        repo = FakeRepository(session)
        instances.append(repo)
        return repo

    monkeypatch.setattr(handler_module, "PaymentRepository", factory)
    return instances


@pytest.fixture
def patch_duplicate_repo(monkeypatch):
    instances: list[FakeDuplicateRepository] = []

    def factory(session):
        repo = FakeDuplicateRepository(session)
        instances.append(repo)
        return repo

    monkeypatch.setattr(handler_module, "PaymentRepository", factory)
    return instances


async def test_handler_opens_session_and_calls_repository(patch_repo) -> None:
    factory = FakeSessionFactory()
    handler = handler_module.build_payment_event_handler(factory)

    await handler(_payload())

    assert len(factory.sessions) == 1
    assert factory.sessions[0].closed is True  # context manager closed
    assert len(patch_repo) == 1
    assert patch_repo[0].calls[0].transactionId == "TX-1"


async def test_handler_handles_duplicate_path(patch_duplicate_repo, caplog) -> None:
    factory = FakeSessionFactory()
    handler = handler_module.build_payment_event_handler(factory)

    await handler(_payload())

    assert len(patch_duplicate_repo) == 1
    messages = [r.getMessage() for r in caplog.records]
    assert any("duplicate" in m for m in messages)


async def test_handler_persists_refunded_with_minimal_fields(patch_repo) -> None:
    """REFUNDED with only message+invoiceId still goes through the repository."""
    refund = PaymentWebhookPayload(
        status=PaymentWebhookStatus.REFUNDED,
        message="Reembolso emitido",
        invoiceId="INV-9",
    )
    factory = FakeSessionFactory()
    handler = handler_module.build_payment_event_handler(factory)

    await handler(refund)

    assert len(patch_repo) == 1
    saved = patch_repo[0].calls[0]
    assert saved.status == PaymentWebhookStatus.REFUNDED
    assert saved.invoiceId == "INV-9"
    assert saved.transactionId is None
    assert saved.amount is None
