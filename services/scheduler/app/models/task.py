import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base
from app.utils.enums import AttemptPhase, Recurrence, TaskStatus
from app.utils.types import TZDateTime


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255))
    execution_time: Mapped[datetime] = mapped_column(TZDateTime)
    webhook_url: Mapped[str] = mapped_column(String(1024))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    recurrence: Mapped[Recurrence] = mapped_column(String(16), default=Recurrence.NONE)
    cron_expr: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[TaskStatus] = mapped_column(String(16), default=TaskStatus.CREATED)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    check_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    parent_task_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(
        TZDateTime, default=_now, onupdate=_now
    )

    attempts: Mapped[list["TaskAttempt"]] = relationship(
        back_populates="task", cascade="all, delete-orphan"
    )


class TaskAttempt(Base):
    __tablename__ = "task_attempts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"))
    attempt_number: Mapped[int] = mapped_column(Integer)
    phase: Mapped[AttemptPhase] = mapped_column(
        String(16), default=AttemptPhase.EXECUTE
    )
    started_at: Mapped[datetime] = mapped_column(TZDateTime, default=_now)
    finished_at: Mapped[datetime | None] = mapped_column(TZDateTime, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    task: Mapped["Task"] = relationship(back_populates="attempts")
