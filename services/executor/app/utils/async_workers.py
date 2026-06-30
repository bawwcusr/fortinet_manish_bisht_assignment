"""Bounded in-process async completion."""

import asyncio

from app.services.execution_service import complete_after_delay
from app.utils.config import settings
from app.utils.logging import get_logger

log = get_logger("async_workers")
_sem: asyncio.Semaphore | None = None


def _get_semaphore() -> asyncio.Semaphore:
    global _sem
    if _sem is None:
        _sem = asyncio.Semaphore(settings.async_max_in_flight)
    return _sem


def _log_task_result(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        log.warning("async_completion_task_failed", error=str(exc))


def spawn_async_completion(session_factory, execution_id: str, endpoint: str) -> None:
    """Schedule async completion without blocking the 202 response."""
    task = asyncio.create_task(
        _run_completion(session_factory, execution_id, endpoint),
        name=f"async-complete-{execution_id}",
    )
    task.add_done_callback(_log_task_result)


async def _run_completion(session_factory, execution_id: str, endpoint: str) -> None:
    async with _get_semaphore():
        await complete_after_delay(session_factory, execution_id, endpoint)
