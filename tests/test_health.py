from httpx import AsyncClient

from app.main import app
from tests.conftest import FakeConsumerSnapshot


async def test_health_ok(client: AsyncClient) -> None:
    response = await client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_health_consumer_503_when_not_initialized(client: AsyncClient) -> None:
    # No app.state.kafka_consumer attached.
    response = await client.get("/api/v1/health/consumer")
    assert response.status_code == 503
    assert response.json() == {"detail": "consumer not initialized"}


async def test_health_consumer_ok_when_running(
    client_with_consumer: AsyncClient,
) -> None:
    response = await client_with_consumer.get("/api/v1/health/consumer")
    assert response.status_code == 200
    body = response.json()
    assert body["state"] == "running"
    assert body["topic"] == "payments-queue"
    assert body["group"] == "miso-travelhub-worker-payments"


async def test_health_consumer_503_when_errored(client: AsyncClient) -> None:
    app.state.kafka_consumer = FakeConsumerSnapshot(state="errored")
    try:
        response = await client.get("/api/v1/health/consumer")
    finally:
        del app.state.kafka_consumer
    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["state"] == "errored"


async def test_health_consumer_ok_when_disabled(client: AsyncClient) -> None:
    """Disabled is a valid steady state (no broker configured)."""
    app.state.kafka_consumer = FakeConsumerSnapshot(state="disabled")
    try:
        response = await client.get("/api/v1/health/consumer")
    finally:
        del app.state.kafka_consumer
    assert response.status_code == 200
    assert response.json()["state"] == "disabled"
