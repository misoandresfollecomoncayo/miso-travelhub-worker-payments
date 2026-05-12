"""Tests for app/main.py — config logging and the lifespan handler."""

from __future__ import annotations

import logging
import os

import pytest
from fastapi import FastAPI

from app.core.config import get_settings
from app.main import (
    _log_database_config,
    _log_kafka_config,
    _no_db_handler,
    create_app,
    lifespan,
)


@pytest.fixture(autouse=True)
def _reset_settings_cache(monkeypatch):
    for key in list(os.environ):
        if key.startswith("KAFKA_") or key == "DATABASE_URL":
            monkeypatch.delenv(key, raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# --- _log_database_config --------------------------------------------------


def test_log_database_config_warns_when_unset(caplog):
    with caplog.at_level(logging.WARNING, logger="app.main"):
        _log_database_config()
    assert any("DATABASE_URL is not set" in r.getMessage() for r in caplog.records)


def test_log_database_config_logs_host_without_credentials(monkeypatch, caplog):
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://user:secret@db-host:5432/db",
    )
    get_settings.cache_clear()
    with caplog.at_level(logging.INFO, logger="app.main"):
        _log_database_config()
    message = next(r.getMessage() for r in caplog.records if "Database" in r.getMessage())
    assert "secret" not in message
    assert "db-host:5432/db" in message
    assert "postgresql+asyncpg" in message


# --- _log_kafka_config -----------------------------------------------------


def test_log_kafka_config_warns_when_disabled(monkeypatch, caplog):
    monkeypatch.setenv("KAFKA_ENABLED", "false")
    get_settings.cache_clear()
    with caplog.at_level(logging.WARNING, logger="app.main"):
        _log_kafka_config()
    assert any("DISABLED" in r.getMessage() for r in caplog.records)


def test_log_kafka_config_errors_when_misconfigured(monkeypatch, caplog):
    monkeypatch.setenv("KAFKA_ENABLED", "true")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "")
    monkeypatch.setenv("KAFKA_TOPIC", "")
    monkeypatch.setenv("KAFKA_GROUP_ID", "")
    get_settings.cache_clear()
    with caplog.at_level(logging.ERROR, logger="app.main"):
        _log_kafka_config()
    messages = [r.getMessage() for r in caplog.records]
    assert any("misconfigured" in m for m in messages)
    assert any("KAFKA_BOOTSTRAP_SERVERS" in m for m in messages)
    assert any("KAFKA_TOPIC" in m for m in messages)
    assert any("KAFKA_GROUP_ID" in m for m in messages)


def test_log_kafka_config_info_when_ok(monkeypatch, caplog):
    monkeypatch.setenv("KAFKA_ENABLED", "true")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "host:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "payments-queue")
    monkeypatch.setenv("KAFKA_GROUP_ID", "g")
    get_settings.cache_clear()
    with caplog.at_level(logging.INFO, logger="app.main"):
        _log_kafka_config()
    assert any("ENABLED" in r.getMessage() for r in caplog.records)


# --- lifespan --------------------------------------------------------------


async def test_lifespan_attaches_consumer_when_disabled(monkeypatch):
    """With Kafka disabled and no DB, lifespan still attaches a consumer."""
    monkeypatch.setenv("KAFKA_ENABLED", "false")
    get_settings.cache_clear()
    app = FastAPI()
    async with lifespan(app):
        assert hasattr(app.state, "kafka_consumer")
        snap = app.state.kafka_consumer.snapshot()
        assert snap["state"] == "disabled"


async def test_lifespan_swallows_start_failure(monkeypatch, caplog):
    """If start() fails (bad config), lifespan logs and still yields."""
    monkeypatch.setenv("KAFKA_ENABLED", "true")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "")  # forces validation error
    monkeypatch.setenv("KAFKA_TOPIC", "payments-queue")
    monkeypatch.setenv("KAFKA_GROUP_ID", "g")
    get_settings.cache_clear()
    app = FastAPI()
    with caplog.at_level(logging.ERROR, logger="app.main"):
        async with lifespan(app):
            assert hasattr(app.state, "kafka_consumer")
    assert any("failed to start" in r.getMessage() for r in caplog.records)


async def test_no_db_handler_raises():
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        await _no_db_handler(object())


# --- create_app smoke ------------------------------------------------------


def test_create_app_mounts_routes(monkeypatch):
    monkeypatch.setenv("KAFKA_ENABLED", "false")
    get_settings.cache_clear()
    app = create_app()
    assert isinstance(app, FastAPI)
    paths = {route.path for route in app.routes}
    assert "/api/v1/health" in paths
    assert "/api/v1/health/consumer" in paths
