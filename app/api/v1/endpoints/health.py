from fastapi import APIRouter, HTTPException, Request, status

from app.services.kafka_consumer import KafkaPaymentConsumer

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe for Cloud Run."""
    return {"status": "ok"}


@router.get("/health/consumer")
async def health_consumer(request: Request) -> dict[str, object]:
    """Readiness probe for the Kafka consumer background task."""
    consumer: KafkaPaymentConsumer | None = getattr(
        request.app.state, "kafka_consumer", None
    )
    if consumer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="consumer not initialized",
        )
    snapshot = consumer.snapshot()
    if snapshot["state"] not in {"running", "disabled"}:
        # 'errored' or 'stopped' → not ready.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=snapshot,
        )
    return snapshot
