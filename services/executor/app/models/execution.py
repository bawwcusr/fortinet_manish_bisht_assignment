import uuid
from datetime import datetime, timezone
from sqlalchemy import JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from app.db import Base
from app.utils.enums import ExecutionStatus
from app.utils.types import TZDateTime


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Execution(Base):
    __tablename__ = "executions"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    endpoint: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[ExecutionStatus] = mapped_column(
        String(16), default=ExecutionStatus.QUEUED
    )
    created_at: Mapped[datetime] = mapped_column(TZDateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(
        TZDateTime, default=_now, onupdate=_now
    )
    logs: Mapped[str | None] = mapped_column(Text, nullable=True)
