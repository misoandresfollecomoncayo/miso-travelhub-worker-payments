from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "miso-travelhub-worker-payments"
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_debug: bool = False

    cors_origins: list[str] = Field(default_factory=lambda: ["*"])

    # PostgreSQL — destino de los eventos consumidos.
    database_url: str = ""
    database_echo: bool = False

    # Kafka consumer — origen de los eventos.
    kafka_enabled: bool = False
    kafka_bootstrap_servers: str = ""
    kafka_topic: str = "payments-queue"
    kafka_group_id: str = "miso-travelhub-worker-payments"
    kafka_client_id: str = "miso-travelhub-worker-payments"
    # earliest | latest
    kafka_auto_offset_reset: str = "earliest"
    kafka_session_timeout_ms: int = 30000
    kafka_max_poll_interval_ms: int = 300000
    # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
    kafka_security_protocol: str = "PLAINTEXT"
    # Solo si KAFKA_SECURITY_PROTOCOL incluye SASL.
    kafka_sasl_mechanism: str = ""
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    # Backoff entre reinicios del loop tras un error inesperado.
    kafka_restart_backoff_seconds: float = 5.0

    # Notification service (Cloud Run sibling). When a payment event is
    # consumed with status=APPROVED we POST {booking_id, status: "PAID"}
    # to NOTIFICATION_SERVICE_URL + NOTIFICATION_SERVICE_PATH.
    notification_enabled: bool = False
    notification_service_url: str = (
        "https://notification-services-154299161799.us-central1.run.app"
    )
    notification_service_path: str = "/api/v1/notifications/send-notification"
    notification_timeout_seconds: float = 5.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
