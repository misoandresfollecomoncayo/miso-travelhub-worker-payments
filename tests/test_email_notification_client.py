"""Unit tests for HttpEmailNotificationClient.

Uses httpx.MockTransport so the client logic is exercised end-to-end
without hitting the real notification-services Cloud Run.
"""

from __future__ import annotations

import json
from decimal import Decimal

import httpx

from app.core.config import Settings
from app.services.email_notification_client import (
    HttpEmailNotificationClient,
    NoopEmailNotificationClient,
    PAYMENT_PROVIDER_NAME,
)


URL = "https://notify.example.com/api/v1/notifications/events"


def _settings(**overrides) -> Settings:
    base = dict(
        email_notification_enabled=True,
        email_notification_url=URL,
        email_notification_timeout_seconds=2.0,
    )
    base.update(overrides)
    return Settings(**base)


async def test_sends_expected_payload() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content.decode("utf-8"))
        captured["content_type"] = request.headers.get("content-type")
        captured["x_internal_token"] = request.headers.get("x-internal-token")
        return httpx.Response(200, json={"ok": True})

    client = HttpEmailNotificationClient(_settings())
    # Default settings have no token, so no header should be sent.
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    try:
        result = await client.send_payment_completed(
            user_id="user-123",
            payment_id="TX-456",
            booking_id="BK-42",
            amount=Decimal("123.45"),
            currency="COP",
        )
    finally:
        await client.stop()

    assert result is True
    assert captured["method"] == "POST"
    assert captured["url"] == URL
    assert "application/json" in captured["content_type"]
    # No token configured → header must be absent.
    assert captured["x_internal_token"] is None
    assert captured["body"] == {
        "event_type": "payment.completed",
        "user_id": "user-123",
        "payload": {
            "payment_id": "TX-456",
            "booking_id": "BK-42",
            "amount": "123.45",  # Decimal → str (preserves precision)
            "currency": "COP",
            "provider": PAYMENT_PROVIDER_NAME,
        },
    }


async def test_amount_int_serializes_as_number() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(200)

    client = HttpEmailNotificationClient(_settings())
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    try:
        await client.send_payment_completed(
            user_id="u",
            payment_id="p",
            booking_id="b",
            amount=123,
            currency="USD",
        )
    finally:
        await client.stop()

    assert captured["body"]["payload"]["amount"] == 123


async def test_returns_false_on_non_2xx() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    client = HttpEmailNotificationClient(_settings())
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    try:
        result = await client.send_payment_completed(
            user_id="u", payment_id="p", booking_id="b", amount=1, currency="COP"
        )
    finally:
        await client.stop()

    assert result is False


async def test_returns_false_on_http_error() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = HttpEmailNotificationClient(_settings())
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    try:
        result = await client.send_payment_completed(
            user_id="u", payment_id="p", booking_id="b", amount=1, currency="COP"
        )
    finally:
        await client.stop()

    assert result is False


async def test_start_skips_when_disabled() -> None:
    client = HttpEmailNotificationClient(_settings(email_notification_enabled=False))

    await client.start()

    assert client._client is None
    result = await client.send_payment_completed(
        user_id="u", payment_id="p", booking_id="b", amount=1, currency="COP"
    )
    assert result is False


async def test_start_then_stop_lifecycle() -> None:
    client = HttpEmailNotificationClient(_settings())

    await client.start()
    assert client._client is not None

    await client.stop()
    assert client._client is None


async def test_start_is_idempotent() -> None:
    client = HttpEmailNotificationClient(_settings())

    await client.start()
    first = client._client
    await client.start()
    assert client._client is first

    await client.stop()


async def test_noop_email_client() -> None:
    client = NoopEmailNotificationClient()
    assert (
        await client.send_payment_completed(
            user_id="u", payment_id="p", booking_id="b", amount=1, currency="COP"
        )
        is False
    )


# ----- x-internal-token header -----


async def test_start_configures_x_internal_token_header() -> None:
    client = HttpEmailNotificationClient(
        _settings(email_notification_internal_token="secret-token-123")
    )

    await client.start()

    try:
        assert client._client is not None
        assert client._client.headers.get("x-internal-token") == "secret-token-123"
    finally:
        await client.stop()


async def test_start_omits_header_when_token_empty(caplog) -> None:
    client = HttpEmailNotificationClient(
        _settings(email_notification_internal_token="")
    )

    await client.start()

    try:
        assert client._client is not None
        # No header should be configured.
        assert "x-internal-token" not in client._client.headers
        # And a loud warning should have been emitted.
        assert any(
            "EMAIL_NOTIFICATION_INTERNAL_TOKEN is empty" in r.getMessage()
            for r in caplog.records
        )
    finally:
        await client.stop()


async def test_start_strips_whitespace_from_token() -> None:
    client = HttpEmailNotificationClient(
        _settings(email_notification_internal_token="  spaced-token  ")
    )

    await client.start()

    try:
        assert client._client.headers.get("x-internal-token") == "spaced-token"
    finally:
        await client.stop()


async def test_header_is_sent_on_each_request() -> None:
    """End-to-end: header configured on client → arrives at the receiver."""
    captured: dict[str, str | None] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["x_internal_token"] = request.headers.get("x-internal-token")
        return httpx.Response(200)

    client = HttpEmailNotificationClient(
        _settings(email_notification_internal_token="my-token-xyz")
    )
    # Mimic what .start() builds: an AsyncClient with the header preset.
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        headers={"x-internal-token": "my-token-xyz"},
    )

    try:
        await client.send_payment_completed(
            user_id="u",
            payment_id="p",
            booking_id="b",
            amount=1,
            currency="COP",
        )
    finally:
        await client.stop()

    assert captured["x_internal_token"] == "my-token-xyz"
