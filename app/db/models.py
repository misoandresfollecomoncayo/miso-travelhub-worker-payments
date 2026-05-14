from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, CHAR, DateTime, Numeric, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Payment(Base):
    """Persisted payment event.

    Columns that are optional carry NULL for ``REFUNDED`` events where the
    payment gateway no longer sends transaction-level data (no card, no
    transaction id, etc.). For every other status the producer guarantees
    these fields are present.
    """

    __tablename__ = "payment"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    message: Mapped[str] = mapped_column(String(512), nullable=False)
    invoice_id: Mapped[str] = mapped_column(String(64), nullable=False)

    # Optional for REFUNDED events; required for everything else (enforced
    # by the Pydantic schema at the edge).
    amount: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    currency: Mapped[str | None] = mapped_column(CHAR(3), nullable=True)
    card_holder: Mapped[str | None] = mapped_column(String(128), nullable=True)
    masked_card: Mapped[str | None] = mapped_column(String(32), nullable=True)
    transaction_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
