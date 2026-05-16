"""HTTP client that notifies the notification-services Cloud Run service.

Called by the payment event handler after a payment is persisted with
status=APPROVED. Best-effort: failures are logged but do not stop the
consumer or block offset commits — the source of truth is already in DB.
"""

from __future__ import annotations

import logging
from typing import Protocol

import httpx

from app.core.config import Settings

logger = logging.getLogger(__name__)


class NotificationClient(Protocol):
    async def notify_booking_paid(self, booking_id: str) -> bool: ...


class HttpNotificationClient:
    """Long-lived httpx-based notification client.

    Built once at app startup, shared across all consumed messages.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        if self._client is not None:
            return
        if not self._settings.notification_enabled:
            logger.warning(
                "Notification client DISABLED (NOTIFICATION_ENABLED=false). "
                "Booking-paid notifications will be skipped."
            )
            return
        self._client = httpx.AsyncClient(
            base_url=self._settings.notification_service_url,
            timeout=self._settings.notification_timeout_seconds,
        )
        logger.info(
            "Notification client started base_url=%s path=%s timeout=%ss",
            self._settings.notification_service_url,
            self._settings.notification_service_path,
            self._settings.notification_timeout_seconds,
        )

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            logger.info("Notification client stopped")
        self._client = None

    async def notify_booking_paid(self, booking_id: str) -> bool:
        """POST {booking_id, status: "PAID"} to the notification service.

        Returns True on 2xx, False otherwise. Never raises.
        """
        if not self._settings.notification_enabled or self._client is None:
            logger.warning(
                "Notification skipped (disabled) booking_id=%s", booking_id
            )
            return False

        body = {"booking_id": booking_id, "status": "PAID"}
        try:
            response = await self._client.post(
                self._settings.notification_service_path, json=body
            )
        except httpx.HTTPError as exc:
            logger.exception(
                "Notification HTTP error booking_id=%s: %s", booking_id, exc
            )
            return False

        if response.is_success:
            logger.info(
                "Notification sent booking_id=%s status_code=%d",
                booking_id,
                response.status_code,
            )
            return True

        logger.warning(
            "Notification non-2xx booking_id=%s status_code=%d body=%s",
            booking_id,
            response.status_code,
            response.text[:200],
        )
        return False


class NoopNotificationClient:
    """Fallback used when notifications are disabled — never raises."""

    async def notify_booking_paid(self, booking_id: str) -> bool:
        logger.debug("notification noop booking_id=%s", booking_id)
        return False
