"""Shared fixtures for worker tests.

These avoid hitting Kafka or PostgreSQL by:
  * stubbing the AIOKafkaConsumer via monkeypatch when needed (per-test),
  * attaching a fake KafkaPaymentConsumer to app.state for HTTP tests so the
    lifespan doesn't try to talk to a real broker.
"""

from __future__ import annotations

import os
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import app


class FakeConsumerSnapshot:
    """Minimal stand-in for KafkaPaymentConsumer that only needs `snapshot()`.

    Used by the /health/consumer endpoint tests.
    """

    def __init__(self, state: str = "running") -> None:
        self._state = state

    def snapshot(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "processed": 0,
            "invalid": 0,
            "errors": 0,
            "topic": "payments-queue",
            "group": "miso-travelhub-worker-payments",
        }


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch):
    """Strip KAFKA_* / DATABASE_URL env vars to avoid leaking shell state."""
    for key in list(os.environ):
        if key.startswith("KAFKA_") or key == "DATABASE_URL":
            monkeypatch.delenv(key, raising=False)


@pytest_asyncio.fixture
async def client() -> AsyncClient:
    """HTTP client that does NOT run the lifespan (no real Kafka/DB)."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


@pytest_asyncio.fixture
async def client_with_consumer(monkeypatch) -> AsyncClient:
    """HTTP client with a fake consumer pre-attached to app.state."""
    app.state.kafka_consumer = FakeConsumerSnapshot(state="running")
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as ac:
            yield ac
    finally:
        if hasattr(app.state, "kafka_consumer"):
            del app.state.kafka_consumer
