"""Read-only access to the ``reserva`` table.

The bookings table is owned by another service but lives in the same
Postgres instance as the worker's payments tables. We only do a single
lookup here: given a ``booking_id`` (which we receive as ``invoiceId`` in
the payment webhook), return the ``viajeroId`` so downstream notifications
can target the right user.

We deliberately use a plain ``text()`` query instead of declaring an ORM
model — this worker doesn't own the schema, and a model would create the
illusion that it does. If the upstream service ever migrates the column,
the breakage stays localized to this one query.
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class BookingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_viajero_id(self, booking_id: str) -> str | None:
        """Return the ``viajeroId`` for a given booking, or None if not found.

        Never raises on "not found" — returns None and lets the caller
        decide what to do (skip notification, log, etc.).
        """
        result = await self._session.execute(
            text('SELECT "viajeroId" FROM reserva WHERE id = :booking_id'),
            {"booking_id": str(booking_id)},
        )
        row = result.first()
        if row is None:
            logger.warning(
                "reserva not found booking_id=%s; cannot resolve viajeroId",
                booking_id,
            )
            return None
        return str(row[0]) if row[0] is not None else None
