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


# ----- Notification client wiring -----


class FakeNotifier:
    def __init__(self, return_value: bool = True, raises: bool = False) -> None:
        self.calls: list[str] = []
        self.return_value = return_value
        self.raises = raises

    async def notify_booking_paid(self, booking_id: str) -> bool:
        self.calls.append(booking_id)
        if self.raises:
            raise RuntimeError("notifier exploded")
        return self.return_value


async def test_handler_notifies_when_approved_payment_persisted(patch_repo) -> None:
    factory = FakeSessionFactory()
    notifier = FakeNotifier()
    handler = handler_module.build_payment_event_handler(factory, notifier=notifier)

    payload = _payload()  # APPROVED, invoiceId=INV-1
    await handler(payload)

    assert notifier.calls == ["INV-1"]


async def test_handler_does_not_notify_when_duplicate(patch_duplicate_repo) -> None:
    """Re-delivery of the same APPROVED event must not double-notify."""
    factory = FakeSessionFactory()
    notifier = FakeNotifier()
    handler = handler_module.build_payment_event_handler(factory, notifier=notifier)

    await handler(_payload())  # repository returns None (duplicate)

    assert notifier.calls == []


async def test_handler_does_not_notify_on_declined(patch_repo) -> None:
    declined = PaymentWebhookPayload(
        status=PaymentWebhookStatus.DECLINED,
        message="rejected",
        invoiceId="INV-2",
        amount=Decimal("10.00"),
        currency="COP",
        cardHolder="X",
        maskedCard="**** **** **** 0000",
        transactionId="TX-DEC",
        processedAt=datetime(2026, 5, 2, 18, 13, 14, tzinfo=timezone.utc),
    )
    factory = FakeSessionFactory()
    notifier = FakeNotifier()
    handler = handler_module.build_payment_event_handler(factory, notifier=notifier)

    await handler(declined)

    assert notifier.calls == []


async def test_handler_does_not_notify_on_refunded(patch_repo) -> None:
    refund = PaymentWebhookPayload(
        status=PaymentWebhookStatus.REFUNDED,
        message="Reembolso emitido",
        invoiceId="INV-9",
    )
    factory = FakeSessionFactory()
    notifier = FakeNotifier()
    handler = handler_module.build_payment_event_handler(factory, notifier=notifier)

    await handler(refund)

    assert notifier.calls == []


async def test_handler_swallows_notification_exceptions(patch_repo, caplog) -> None:
    """A buggy notifier must not bubble up — offset must still commit."""
    factory = FakeSessionFactory()
    notifier = FakeNotifier(raises=True)
    handler = handler_module.build_payment_event_handler(factory, notifier=notifier)

    # Should NOT raise even though notifier does.
    await handler(_payload())

    assert notifier.calls == ["INV-1"]
    assert any(
        "notification raised unexpectedly" in r.getMessage()
        for r in caplog.records
    )


async def test_handler_logs_warning_when_notification_returns_false(
    patch_repo, caplog
) -> None:
    factory = FakeSessionFactory()
    notifier = FakeNotifier(return_value=False)
    handler = handler_module.build_payment_event_handler(factory, notifier=notifier)

    await handler(_payload())

    assert notifier.calls == ["INV-1"]
    assert any(
        "booking-paid notification failed" in r.getMessage()
        for r in caplog.records
    )


# ----- Email notification client wiring (payment.completed) -----


class FakeEmailNotifier:
    def __init__(
        self, return_value: bool = True, raises: bool = False
    ) -> None:
        self.calls: list[dict] = []
        self.return_value = return_value
        self.raises = raises

    async def send_payment_completed(self, **kwargs) -> bool:  # noqa: ANN003
        self.calls.append(kwargs)
        if self.raises:
            raise RuntimeError("email notifier exploded")
        return self.return_value


class FakeBookingRepo:
    def __init__(self, viajero_id: str | None = "user-987", raises: bool = False) -> None:
        self.viajero_id = viajero_id
        self.raises = raises
        self.calls: list[str] = []

    async def get_viajero_id(self, booking_id: str) -> str | None:
        self.calls.append(booking_id)
        if self.raises:
            raise RuntimeError("db unreachable")
        return self.viajero_id


@pytest.fixture
def patch_booking_repo(monkeypatch):
    instances: list[FakeBookingRepo] = []

    def factory(session, viajero_id="user-987", raises=False):  # noqa: ANN001
        repo = FakeBookingRepo(viajero_id=viajero_id, raises=raises)
        instances.append(repo)
        return repo

    return _BookingRepoPatcher(monkeypatch, instances, factory)


class _BookingRepoPatcher:
    """Lets each test configure how the next-created BookingRepo behaves."""

    def __init__(self, monkeypatch, instances, factory) -> None:
        self.monkeypatch = monkeypatch
        self.instances = instances
        self._factory = factory
        self._viajero_id: str | None = "user-987"
        self._raises = False
        self._installed = False

    def with_viajero_id(self, value: str | None) -> "_BookingRepoPatcher":
        self._viajero_id = value
        self._install()
        return self

    def with_raises(self) -> "_BookingRepoPatcher":
        self._raises = True
        self._install()
        return self

    def _install(self) -> None:
        if self._installed:
            return

        def create(session):  # noqa: ANN001
            repo = FakeBookingRepo(
                viajero_id=self._viajero_id, raises=self._raises
            )
            self.instances.append(repo)
            return repo

        self.monkeypatch.setattr(handler_module, "BookingRepository", create)
        self._installed = True

    def __iter__(self):
        return iter(self.instances)

    def __len__(self):
        return len(self.instances)

    def __getitem__(self, idx):
        return self.instances[idx]


async def test_handler_sends_email_when_approved_payment_persisted(
    patch_repo, patch_booking_repo
) -> None:
    patch_booking_repo.with_viajero_id("user-987")
    factory = FakeSessionFactory()
    notifier = FakeNotifier()
    email_notifier = FakeEmailNotifier()
    handler = handler_module.build_payment_event_handler(
        factory, notifier=notifier, email_notifier=email_notifier
    )

    await handler(_payload())  # APPROVED, INV-1, amount=123.45, COP, TX-1

    # Two sessions: one for PaymentRepository, one for BookingRepository lookup.
    assert len(factory.sessions) == 2
    assert len(patch_booking_repo) == 1
    assert patch_booking_repo[0].calls == ["INV-1"]

    assert len(email_notifier.calls) == 1
    call = email_notifier.calls[0]
    assert call["user_id"] == "user-987"
    assert call["payment_id"] == "TX-1"
    assert call["booking_id"] == "INV-1"
    assert call["amount"] == Decimal("123.45")
    assert call["currency"] == "COP"


async def test_handler_does_not_email_when_duplicate(
    patch_duplicate_repo, patch_booking_repo
) -> None:
    patch_booking_repo.with_viajero_id("user-987")
    factory = FakeSessionFactory()
    email_notifier = FakeEmailNotifier()
    handler = handler_module.build_payment_event_handler(
        factory, email_notifier=email_notifier
    )

    await handler(_payload())

    assert email_notifier.calls == []
    assert len(patch_booking_repo) == 0  # never opened the lookup session


async def test_handler_does_not_email_on_declined(
    patch_repo, patch_booking_repo
) -> None:
    patch_booking_repo.with_viajero_id("user-987")
    declined = PaymentWebhookPayload(
        status=PaymentWebhookStatus.DECLINED,
        message="rejected",
        invoiceId="INV-2",
        amount=Decimal("10.00"),
        currency="COP",
        cardHolder="X",
        maskedCard="**** **** **** 0000",
        transactionId="TX-DEC",
        processedAt=datetime(2026, 5, 2, 18, 13, 14, tzinfo=timezone.utc),
    )
    factory = FakeSessionFactory()
    email_notifier = FakeEmailNotifier()
    handler = handler_module.build_payment_event_handler(
        factory, email_notifier=email_notifier
    )

    await handler(declined)

    assert email_notifier.calls == []


async def test_handler_skips_email_when_viajero_not_found(
    patch_repo, patch_booking_repo
) -> None:
    """Booking missing → user_id is None → don't fire the email."""
    patch_booking_repo.with_viajero_id(None)
    factory = FakeSessionFactory()
    email_notifier = FakeEmailNotifier()
    handler = handler_module.build_payment_event_handler(
        factory, email_notifier=email_notifier
    )

    await handler(_payload())

    assert email_notifier.calls == []  # never called


async def test_handler_swallows_lookup_exceptions(
    patch_repo, patch_booking_repo, caplog
) -> None:
    """A DB error during lookup must not break the handler."""
    patch_booking_repo.with_raises()
    factory = FakeSessionFactory()
    email_notifier = FakeEmailNotifier()
    handler = handler_module.build_payment_event_handler(
        factory, email_notifier=email_notifier
    )

    # Should NOT raise.
    await handler(_payload())

    assert email_notifier.calls == []
    assert any(
        "viajeroId lookup failed" in r.getMessage() for r in caplog.records
    )


async def test_handler_swallows_email_exceptions(
    patch_repo, patch_booking_repo, caplog
) -> None:
    patch_booking_repo.with_viajero_id("user-987")
    factory = FakeSessionFactory()
    email_notifier = FakeEmailNotifier(raises=True)
    handler = handler_module.build_payment_event_handler(
        factory, email_notifier=email_notifier
    )

    # Should NOT raise even though notifier does.
    await handler(_payload())

    assert len(email_notifier.calls) == 1
    assert any(
        "email notification raised unexpectedly" in r.getMessage()
        for r in caplog.records
    )


async def test_handler_logs_warning_when_email_returns_false(
    patch_repo, patch_booking_repo, caplog
) -> None:
    patch_booking_repo.with_viajero_id("user-987")
    factory = FakeSessionFactory()
    email_notifier = FakeEmailNotifier(return_value=False)
    handler = handler_module.build_payment_event_handler(
        factory, email_notifier=email_notifier
    )

    await handler(_payload())

    assert len(email_notifier.calls) == 1
    assert any(
        "payment.completed email failed" in r.getMessage()
        for r in caplog.records
    )


async def test_handler_fires_both_channels_independently(
    patch_repo, patch_booking_repo
) -> None:
    """Push notifier failing must not stop the email, and vice versa."""
    patch_booking_repo.with_viajero_id("user-987")
    factory = FakeSessionFactory()
    notifier = FakeNotifier(return_value=False)  # push fails
    email_notifier = FakeEmailNotifier(return_value=True)  # email ok
    handler = handler_module.build_payment_event_handler(
        factory, notifier=notifier, email_notifier=email_notifier
    )

    await handler(_payload())

    assert notifier.calls == ["INV-1"]
    assert len(email_notifier.calls) == 1  # email still fired despite push failure
