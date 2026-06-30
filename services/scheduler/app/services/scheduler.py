from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.date import DateTrigger
from app.db import SessionLocal
from app.utils.config import settings
from app.utils.logging import get_logger

log = get_logger("scheduler")
_scheduler: AsyncIOScheduler | None = None


def init_scheduler(in_memory: bool = False) -> AsyncIOScheduler:
    global _scheduler
    if in_memory:
        _scheduler = AsyncIOScheduler()
    else:
        jobstores = {"default": SQLAlchemyJobStore(url=settings.sync_database_url)}
        _scheduler = AsyncIOScheduler(jobstores=jobstores)
    return _scheduler


def get_scheduler() -> AsyncIOScheduler:
    if _scheduler is None:
        init_scheduler()
    return _scheduler


async def _run_execute(task_id: str) -> None:
    # Lazy import avoids scheduler <-> execution circular import at module load.
    from app.services.execution import execute_task

    await execute_task(task_id, session_factory=SessionLocal)


def register_task_job(task) -> None:
    get_scheduler().add_job(
        _run_execute,
        trigger=DateTrigger(run_date=task.execution_time),
        args=[task.id],
        id=task.id,
        replace_existing=True,
        misfire_grace_time=settings.scheduler_misfire_grace_seconds,
    )


async def _run_poll(task_id: str) -> None:
    from app.services.execution import poll_task

    await poll_task(task_id, session_factory=SessionLocal)


def register_poll_job(task) -> None:
    # One-shot poll job; execution re-schedules if still pending (see poll_task).
    run_at = datetime.now(timezone.utc) + timedelta(
        seconds=settings.poll_interval_seconds
    )
    get_scheduler().add_job(
        _run_poll,
        trigger=DateTrigger(run_date=run_at),
        args=[task.id],
        id=f"poll:{task.id}",
        replace_existing=True,
        misfire_grace_time=settings.scheduler_misfire_grace_seconds,
    )


def remove_task_job(task_id: str) -> None:
    # Cancel may need to drop both the execute job and an in-flight poll job.
    scheduler = get_scheduler()
    if scheduler.get_job(task_id):
        scheduler.remove_job(task_id)
    poll_id = f"poll:{task_id}"
    if scheduler.get_job(poll_id):
        scheduler.remove_job(poll_id)


def start() -> None:
    get_scheduler().start()
    log.info("scheduler_started")


def shutdown() -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
