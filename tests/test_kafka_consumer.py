"""Unit tests for KafkaPaymentConsumer with a fake AIOKafkaConsumer.

These tests never reach a real broker — `aiokafka.AIOKafkaConsumer` is
monkeypatched, and the fake consumer is primed with messages that drain
to completion so the supervised run() loop exits cleanly.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from app.core.config import Settings
from app.services.kafka_consumer import (
    KafkaConsumerConfigError,
    KafkaPaymentConsumer,
)


# --- helpers ----------------------------------------------------------------


def _settings(**overrides) -> Settings:
    defaults: dict[str, Any] = {
        "kafka_enabled": True,
        "kafka_bootstrap_servers": "broker:9092",
        "kafka_topic": "payments-queue",
        "kafka_group_id": "test-group",
        "kafka_client_id": "test-client",
        "kafka_auto_offset_reset": "earliest",
        "kafka_session_timeout_ms": 30000,
        "kafka_max_poll_interval_ms": 300000,
        "kafka_security_protocol": "PLAINTEXT",
        "kafka_sasl_mechanism": "",
        "kafka_sasl_username": "",
        "kafka_sasl_password": "",
        "kafka_restart_backoff_seconds": 0.01,
    }
    defaults.update(overrides)
    return Settings(**defaults)


def _valid_message_json(tx_id: str = "TX-1") -> bytes:
    return (
        '{"status":"APPROVED","message":"ok","invoiceId":"INV-1",'
        '"amount":"100.00","currency":"COP","cardHolder":"JOHN",'
        '"maskedCard":"**** **** **** 1234",'
        f'"transactionId":"{tx_id}",'
        '"processedAt":"2026-05-02T18:13:14.424Z"}'
    ).encode("utf-8")


def _fake_msg(value: bytes, offset: int = 0, key: bytes = b"k") -> SimpleNamespace:
    return SimpleNamespace(
        topic="payments-queue",
        partition=0,
        offset=offset,
        key=key,
        value=value,
    )


class FakeAIOKafkaConsumer:
    """Drop-in replacement for aiokafka.AIOKafkaConsumer."""

    def __init__(self, *topics, **kwargs):
        self.topics = topics
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.commits: list[int] = []
        self.messages: list[Any] = []
        # Optional: raise this on the next __anext__ call.
        self.raise_on_next: BaseException | None = None

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def commit(self) -> None:
        self.commits.append(len(self.commits))

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.raise_on_next is not None:
            exc = self.raise_on_next
            self.raise_on_next = None
            raise exc
        if not self.messages:
            raise StopAsyncIteration
        return self.messages.pop(0)


@pytest.fixture
def fake_kafka(monkeypatch):
    holder: dict[str, FakeAIOKafkaConsumer] = {}

    def factory(*topics, **kwargs):
        instance = FakeAIOKafkaConsumer(*topics, **kwargs)
        holder["instance"] = instance
        return instance

    monkeypatch.setattr("aiokafka.AIOKafkaConsumer", factory)
    return holder


# --- _validate / config -----------------------------------------------------


async def test_start_raises_when_bootstrap_missing() -> None:
    consumer = KafkaPaymentConsumer(
        _settings(kafka_bootstrap_servers=""), _noop_handler
    )
    with pytest.raises(KafkaConsumerConfigError, match="BOOTSTRAP"):
        await consumer.start()


async def test_start_raises_when_topic_missing() -> None:
    consumer = KafkaPaymentConsumer(
        _settings(kafka_topic=""), _noop_handler
    )
    with pytest.raises(KafkaConsumerConfigError, match="TOPIC"):
        await consumer.start()


async def test_start_raises_when_group_missing() -> None:
    consumer = KafkaPaymentConsumer(
        _settings(kafka_group_id=""), _noop_handler
    )
    with pytest.raises(KafkaConsumerConfigError, match="GROUP_ID"):
        await consumer.start()


async def test_start_raises_on_unknown_protocol() -> None:
    consumer = KafkaPaymentConsumer(
        _settings(kafka_security_protocol="WEIRD"), _noop_handler
    )
    with pytest.raises(KafkaConsumerConfigError, match="security_protocol"):
        await consumer.start()


async def test_start_raises_on_incomplete_sasl() -> None:
    consumer = KafkaPaymentConsumer(
        _settings(
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism="",
            kafka_sasl_username="",
            kafka_sasl_password="",
        ),
        _noop_handler,
    )
    with pytest.raises(KafkaConsumerConfigError, match="SASL"):
        await consumer.start()


async def test_start_passes_sasl_credentials(fake_kafka) -> None:
    consumer = KafkaPaymentConsumer(
        _settings(
            kafka_security_protocol="SASL_PLAINTEXT",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_username="u",
            kafka_sasl_password="p",
        ),
        _noop_handler,
    )
    await consumer.start()
    kwargs = fake_kafka["instance"].kwargs
    assert kwargs["sasl_mechanism"] == "PLAIN"
    assert kwargs["sasl_plain_username"] == "u"
    assert kwargs["sasl_plain_password"] == "p"


# --- start / stop -----------------------------------------------------------


async def test_start_no_op_when_disabled(fake_kafka) -> None:
    consumer = KafkaPaymentConsumer(
        _settings(kafka_enabled=False), _noop_handler
    )
    await consumer.start()
    assert consumer.state == "disabled"
    assert "instance" not in fake_kafka


async def test_start_idempotent(fake_kafka) -> None:
    consumer = KafkaPaymentConsumer(_settings(), _noop_handler)
    await consumer.start()
    first = fake_kafka["instance"]
    await consumer.start()  # second call: no-op
    assert fake_kafka["instance"] is first


async def test_stop_stops_consumer(fake_kafka) -> None:
    consumer = KafkaPaymentConsumer(_settings(), _noop_handler)
    await consumer.start()
    instance = fake_kafka["instance"]
    await consumer.stop()
    assert instance.stopped is True
    assert consumer.state == "stopped"


async def test_stop_is_safe_when_never_started() -> None:
    consumer = KafkaPaymentConsumer(
        _settings(kafka_enabled=False), _noop_handler
    )
    await consumer.stop()  # must not raise


# --- _consume_loop ----------------------------------------------------------


async def test_consume_loop_processes_messages_and_commits(fake_kafka) -> None:
    handled: list = []

    async def handler(payload):
        handled.append(payload)

    consumer = KafkaPaymentConsumer(_settings(), handler)
    await consumer.start()

    instance = fake_kafka["instance"]
    instance.messages = [
        _fake_msg(_valid_message_json("TX-A"), offset=0),
        _fake_msg(_valid_message_json("TX-B"), offset=1),
    ]

    await consumer.run()

    assert [p.transactionId for p in handled] == ["TX-A", "TX-B"]
    assert consumer.processed_count == 2
    assert consumer.invalid_count == 0
    assert len(instance.commits) == 2


async def test_poison_message_is_skipped_and_committed(fake_kafka) -> None:
    async def handler(_payload):
        raise AssertionError("handler should not be called for poison messages")

    consumer = KafkaPaymentConsumer(_settings(), handler)
    await consumer.start()

    instance = fake_kafka["instance"]
    instance.messages = [
        _fake_msg(b"not-json", offset=0),
        _fake_msg(b'{"oops":"missing fields"}', offset=1),
    ]

    await consumer.run()

    assert consumer.invalid_count == 2
    assert consumer.processed_count == 0
    assert len(instance.commits) == 2  # committed to skip both


async def test_handler_failure_restarts_loop_with_backoff(fake_kafka) -> None:
    calls: dict[str, int] = {"n": 0}

    async def handler(_payload):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("DB hiccup")

    consumer = KafkaPaymentConsumer(_settings(), handler)
    await consumer.start()

    instance = fake_kafka["instance"]
    # First batch: the only message will trigger the handler error.
    # After the loop restart, we want it to drain cleanly.
    instance.messages = [_fake_msg(_valid_message_json("TX-1"), offset=0)]

    await consumer.run()

    assert calls["n"] == 1
    assert consumer.error_count == 1
    # The failing message was NOT committed.
    assert len(instance.commits) == 0


async def test_cancelled_error_propagates(fake_kafka) -> None:
    consumer = KafkaPaymentConsumer(_settings(), _noop_handler)
    await consumer.start()

    fake_kafka["instance"].raise_on_next = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await consumer.run()


# --- spawn / supervised lifecycle ------------------------------------------


async def test_spawn_runs_loop_in_background_and_stop_cancels(fake_kafka) -> None:
    handled: list = []

    async def handler(payload):
        handled.append(payload)

    consumer = KafkaPaymentConsumer(_settings(), handler)
    await consumer.start()

    instance = fake_kafka["instance"]
    instance.messages = [_fake_msg(_valid_message_json("TX-Z"), offset=0)]

    task = consumer.spawn()
    assert task is consumer.spawn()  # idempotent

    # Give the loop a chance to drain.
    await asyncio.sleep(0.05)
    await consumer.stop()

    assert handled[0].transactionId == "TX-Z"
    assert instance.stopped is True
    assert consumer.state == "stopped"


async def test_run_is_noop_when_disabled() -> None:
    consumer = KafkaPaymentConsumer(
        _settings(kafka_enabled=False), _noop_handler
    )
    # Should return immediately without doing anything.
    await consumer.run()


# --- snapshot ---------------------------------------------------------------


def test_snapshot_returns_state() -> None:
    consumer = KafkaPaymentConsumer(_settings(), _noop_handler)
    snap = consumer.snapshot()
    assert snap["state"] == "stopped"
    assert snap["topic"] == "payments-queue"
    assert snap["group"] == "test-group"
    assert snap["processed"] == 0


# --- shared no-op handler ---------------------------------------------------


async def _noop_handler(_payload) -> None:
    return None
