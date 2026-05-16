"""Unit tests for BookingRepository.

We stub AsyncSession.execute so we never need a real Postgres — the query
text is asserted to make sure we don't accidentally drift away from the
schema owned by the upstream service.
"""

from __future__ import annotations

from typing import Any

from app.repositories.booking_repository import BookingRepository


class _FakeResult:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def first(self) -> tuple[Any, ...] | None:
        return self._row


class _FakeSession:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row
        self.executed: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, statement, params=None) -> _FakeResult:  # noqa: ANN001
        # SQLAlchemy `text()` wraps the SQL string in a TextClause —
        # ``.text`` gives us the raw SQL for assertion purposes.
        self.executed.append((str(statement.text), dict(params or {})))
        return _FakeResult(self._row)


async def test_get_viajero_id_returns_value_from_row() -> None:
    session = _FakeSession(row=("user-123",))
    repo = BookingRepository(session)  # type: ignore[arg-type]

    result = await repo.get_viajero_id("BK-42")

    assert result == "user-123"
    assert len(session.executed) == 1
    sql, params = session.executed[0]
    assert 'SELECT "viajeroId" FROM reserva' in sql
    assert "WHERE id = :booking_id" in sql
    assert params == {"booking_id": "BK-42"}


async def test_get_viajero_id_coerces_to_str() -> None:
    """SQLAlchemy returns whatever native type — caller should always get str."""
    session = _FakeSession(row=(42,))
    repo = BookingRepository(session)  # type: ignore[arg-type]

    result = await repo.get_viajero_id("BK-42")

    assert result == "42"


async def test_get_viajero_id_returns_none_when_no_row(caplog) -> None:
    session = _FakeSession(row=None)
    repo = BookingRepository(session)  # type: ignore[arg-type]

    result = await repo.get_viajero_id("BK-DOES-NOT-EXIST")

    assert result is None
    assert any("reserva not found" in r.getMessage() for r in caplog.records)


async def test_get_viajero_id_returns_none_when_column_is_null() -> None:
    session = _FakeSession(row=(None,))
    repo = BookingRepository(session)  # type: ignore[arg-type]

    result = await repo.get_viajero_id("BK-42")

    assert result is None


async def test_booking_id_is_always_cast_to_str() -> None:
    """If a caller passes a non-str (e.g. UUID), the query still binds a string."""
    session = _FakeSession(row=("user-1",))
    repo = BookingRepository(session)  # type: ignore[arg-type]

    await repo.get_viajero_id(12345)  # type: ignore[arg-type]

    _, params = session.executed[0]
    assert params == {"booking_id": "12345"}
