"""Unit tests for HttpNotificationClient (worker).

Uses httpx.MockTransport so the client logic is exercised end-to-end
without hitting the real notification-services Cloud Run.
"""

from __future__ import annotations

import json

import httpx

from app.core.config import Settings
from app.services.notification_client import (
    HttpNotificationClient,
    NoopNotificationClient,
)


def _settings(**overrides) -> Settings:
    base = dict(
        notification_enabled=True,
        notification_service_url="https://notify.example.com",
        notification_service_path="/api/v1/notifications/send-notification",
        notification_timeout_seconds=2.0,
    )
    base.update(overrides)
    return Settings(**base)


async def test_notify_booking_paid_posts_expected_payload() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content.decode("utf-8"))
        captured["content_type"] = request.headers.get("content-type")
        return httpx.Response(200, json={"ok": True})

    client = HttpNotificationClient(_settings())
    client._client = httpx.AsyncClient(
        base_url="https://notify.example.com",
        transport=httpx.MockTransport(handler),
    )

    try:
        result = await client.notify_booking_paid(booking_id="BK-42")
    finally:
        await client.stop()

    assert result is True
    assert captured["method"] == "POST"
    assert (
        captured["url"]
        == "https://notify.example.com/api/v1/notifications/send-notification"
    )
    assert captured["body"] == {"booking_id": "BK-42", "status": "PAID"}
    assert "application/json" in captured["content_type"]


async def test_notify_booking_paid_returns_false_on_non_2xx() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="boom")

    client = HttpNotificationClient(_settings())
    client._client = httpx.AsyncClient(
        base_url="https://notify.example.com",
        transport=httpx.MockTransport(handler),
    )

    try:
        result = await client.notify_booking_paid(booking_id="BK-42")
    finally:
        await client.stop()

    assert result is False


async def test_notify_booking_paid_returns_false_on_http_error() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = HttpNotificationClient(_settings())
    client._client = httpx.AsyncClient(
        base_url="https://notify.example.com",
        transport=httpx.MockTransport(handler),
    )

    try:
        result = await client.notify_booking_paid(booking_id="BK-42")
    finally:
        await client.stop()

    assert result is False


async def test_start_skips_when_disabled() -> None:
    client = HttpNotificationClient(_settings(notification_enabled=False))

    await client.start()

    assert client._client is None  # never built
    assert await client.notify_booking_paid("BK-1") is False


async def test_start_then_stop_lifecycle() -> None:
    client = HttpNotificationClient(_settings())

    await client.start()
    assert client._client is not None

    await client.stop()
    assert client._client is None


async def test_start_is_idempotent() -> None:
    client = HttpNotificationClient(_settings())

    await client.start()
    first = client._client
    await client.start()
    assert client._client is first

    await client.stop()


async def test_noop_notification_client() -> None:
    client = NoopNotificationClient()
    assert await client.notify_booking_paid("BK-1") is False
