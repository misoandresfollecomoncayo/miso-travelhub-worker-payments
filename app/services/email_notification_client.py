"""HTTP client that emits ``payment.completed`` events to the email pipeline.

The notification-services Cloud Run exposes a generic events endpoint that
dispatches templated emails based on ``event_type``. Called by the payment
event handler after a payment is persisted with status=APPROVED *and*
after we resolve the ``viajeroId`` from the ``reserva`` table.

Best-effort: failures are logged but do not stop the consumer or block
offset commits — the source of truth is already in the payments table.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Protocol

import httpx

from app.core.config import Settings

logger = logging.getLogger(__name__)


# Currently hardcoded — there's only one upstream gateway during the
# MISO project. Promote to a setting if/when we onboard a second provider.
PAYMENT_PROVIDER_NAME = "PROVIDER DE PRUEBA"


class EmailNotificationClient(Protocol):
    async def send_payment_completed(
        self,
        *,
        user_id: str,
        payment_id: str,
        booking_id: str,
        amount: Decimal | float | int,
        currency: str,
    ) -> bool: ...


class HttpEmailNotificationClient:
    """Long-lived httpx client for the events endpoint.

    Built once at app startup, shared across all consumed messages.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        if self._client is not None:
            return
        if not self._settings.email_notification_enabled:
            logger.warning(
                "Email notification client DISABLED "
                "(EMAIL_NOTIFICATION_ENABLED=false). "
                "payment.completed events will be skipped."
            )
            return
        # The configured URL is the full endpoint (host + path) — we just
        # POST to it directly, no base_url juggling.
        self._client = httpx.AsyncClient(
            timeout=self._settings.email_notification_timeout_seconds,
        )
        logger.info(
            "Email notification client started url=%s timeout=%ss",
            self._settings.email_notification_url,
            self._settings.email_notification_timeout_seconds,
        )

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            logger.info("Email notification client stopped")
        self._client = None

    async def send_payment_completed(
        self,
        *,
        user_id: str,
        payment_id: str,
        booking_id: str,
        amount: Decimal | float | int,
        currency: str,
    ) -> bool:
        """POST a ``payment.completed`` event. Returns True on 2xx, False otherwise.

        Never raises — failures are logged so the caller can keep going.
        """
        if not self._settings.email_notification_enabled or self._client is None:
            logger.warning(
                "Email notification skipped (disabled) booking_id=%s payment_id=%s",
                booking_id,
                payment_id,
            )
            return False

        body: dict[str, Any] = {
            "event_type": "payment.completed",
            "user_id": user_id,
            "payload": {
                "payment_id": payment_id,
                "booking_id": booking_id,
                # Decimal → str preserves precision and serializes cleanly.
                "amount": str(amount) if isinstance(amount, Decimal) else amount,
                "currency": currency,
                "provider": PAYMENT_PROVIDER_NAME,
            },
        }

        try:
            response = await self._client.post(
                self._settings.email_notification_url, json=body
            )
        except httpx.HTTPError as exc:
            logger.exception(
                "Email notification HTTP error booking_id=%s payment_id=%s: %s",
                booking_id,
                payment_id,
                exc,
            )
            return False

        if response.is_success:
            logger.info(
                "Email notification sent booking_id=%s payment_id=%s status_code=%d",
                booking_id,
                payment_id,
                response.status_code,
            )
            return True

        logger.warning(
            "Email notification non-2xx booking_id=%s payment_id=%s "
            "status_code=%d body=%s",
            booking_id,
            payment_id,
            response.status_code,
            response.text[:200],
        )
        return False


class NoopEmailNotificationClient:
    """Fallback used when the email channel is disabled — never raises."""

    async def send_payment_completed(self, **kwargs: Any) -> bool:
        logger.debug("email notification noop %s", kwargs)
        return False
