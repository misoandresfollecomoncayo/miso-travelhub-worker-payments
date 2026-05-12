"""Kafka consumer for payment-webhook events.

Long-running async consumer that pulls messages from a Kafka topic produced
by the payments service. Each message is deserialized into a
``PaymentWebhookPayload`` and handed to a caller-supplied ``on_message``
coroutine for persistence.

Semantics:

- **At-least-once** delivery. Offsets are committed only after ``on_message``
  succeeds. The downstream handler (``PaymentRepository.create_from_webhook``)
  is idempotent on ``transaction_id`` so reprocessing is safe.
- **Poison messages** (malformed payload) are logged and skipped (offset is
  committed so we don't loop on them forever).
- **Transient errors** (handler raised) leave the offset uncommitted; the
  supervisor restarts the consumer after a short backoff so the broker
  re-delivers from the last committed offset.
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable

from pydantic import ValidationError

from app.core.config import Settings
from app.schemas.payment_webhook import PaymentWebhookPayload

logger = logging.getLogger(__name__)


OnMessage = Callable[[PaymentWebhookPayload], Awaitable[None]]


class KafkaConsumerConfigError(RuntimeError):
    pass


class KafkaPaymentConsumer:
    def __init__(self, settings: Settings, on_message: OnMessage) -> None:
        self._settings = settings
        self._on_message = on_message
        self._consumer: Any | None = None
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        # State exposed for /health/consumer.
        self.state: str = "stopped"
        self.processed_count: int = 0
        self.invalid_count: int = 0
        self.error_count: int = 0

    # --- config & lifecycle ------------------------------------------------

    def _validate(self) -> None:
        s = self._settings
        if not s.kafka_bootstrap_servers:
            raise KafkaConsumerConfigError(
                "KAFKA_BOOTSTRAP_SERVERS is empty"
            )
        if not s.kafka_topic:
            raise KafkaConsumerConfigError("KAFKA_TOPIC is empty")
        if not s.kafka_group_id:
            raise KafkaConsumerConfigError("KAFKA_GROUP_ID is empty")
        protocol = s.kafka_security_protocol.upper()
        if protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
            raise KafkaConsumerConfigError(
                f"unsupported security_protocol={protocol}"
            )
        if protocol.startswith("SASL") and (
            not s.kafka_sasl_mechanism
            or not s.kafka_sasl_username
            or not s.kafka_sasl_password
        ):
            raise KafkaConsumerConfigError(
                "SASL requires mechanism + username + password"
            )

    def _build_consumer_kwargs(self) -> dict[str, Any]:
        s = self._settings
        kwargs: dict[str, Any] = {
            "bootstrap_servers": s.kafka_bootstrap_servers,
            "group_id": s.kafka_group_id,
            "client_id": s.kafka_client_id,
            "enable_auto_commit": False,
            "auto_offset_reset": s.kafka_auto_offset_reset,
            "session_timeout_ms": s.kafka_session_timeout_ms,
            "max_poll_interval_ms": s.kafka_max_poll_interval_ms,
            "security_protocol": s.kafka_security_protocol.upper(),
        }
        if kwargs["security_protocol"].startswith("SASL"):
            kwargs["sasl_mechanism"] = s.kafka_sasl_mechanism
            kwargs["sasl_plain_username"] = s.kafka_sasl_username
            kwargs["sasl_plain_password"] = s.kafka_sasl_password
        return kwargs

    async def start(self) -> None:
        s = self._settings
        if not s.kafka_enabled:
            logger.warning(
                "Kafka consumer DISABLED (KAFKA_ENABLED=false). "
                "No messages will be processed."
            )
            self.state = "disabled"
            return
        if self._consumer is not None:
            return

        self._validate()

        from aiokafka import AIOKafkaConsumer

        self._consumer = AIOKafkaConsumer(
            s.kafka_topic, **self._build_consumer_kwargs()
        )
        await self._consumer.start()
        self.state = "running"
        logger.info(
            "Kafka consumer started bootstrap=%s topic=%s group=%s protocol=%s",
            s.kafka_bootstrap_servers,
            s.kafka_topic,
            s.kafka_group_id,
            s.kafka_security_protocol,
        )

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        if self._consumer is not None:
            await self._consumer.stop()
            logger.info("Kafka consumer stopped")
        self._consumer = None
        self.state = "stopped"

    # --- background runner -------------------------------------------------

    def spawn(self) -> asyncio.Task:
        """Start the supervised consume loop as a background task."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run(), name="kafka-consumer")
        return self._task

    async def run(self) -> None:
        """Supervised consume loop: restarts on unexpected errors."""
        s = self._settings
        if not s.kafka_enabled:
            return
        while not self._stop_event.is_set():
            try:
                await self._consume_loop()
                # Consumer drained (test scenario) — exit cleanly.
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "consume loop crashed; restarting in %.1fs",
                    s.kafka_restart_backoff_seconds,
                )
                self.state = "errored"
                self.error_count += 1
                await asyncio.sleep(s.kafka_restart_backoff_seconds)
                self.state = "running"

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        async for msg in self._consumer:
            if self._stop_event.is_set():
                break
            await self._handle_one(msg)

    async def _handle_one(self, msg: Any) -> None:
        assert self._consumer is not None
        try:
            payload = PaymentWebhookPayload.model_validate_json(msg.value)
        except ValidationError:
            logger.exception(
                "invalid payload at topic=%s partition=%s offset=%s key=%s",
                msg.topic,
                msg.partition,
                msg.offset,
                msg.key,
            )
            self.invalid_count += 1
            # Commit so we don't reprocess this poison message forever.
            await self._consumer.commit()
            return

        await self._on_message(payload)
        await self._consumer.commit()
        self.processed_count += 1
        logger.info(
            "payment-event consumed tx=%s status=%s offset=%s",
            payload.transactionId,
            payload.status.value,
            msg.offset,
        )

    # --- introspection -----------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "processed": self.processed_count,
            "invalid": self.invalid_count,
            "errors": self.error_count,
            "topic": self._settings.kafka_topic,
            "group": self._settings.kafka_group_id,
        }
