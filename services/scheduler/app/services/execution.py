"""Webhook execution engine (called by APScheduler, not HTTP).

High-level flows
----------------

**Sync webhook** (executor returns 2xx, not 202):

    execute_task
      → mark RUNNING
      → POST webhook (with retries + TaskAttempt audit rows)
      → mark SUCCESS or FAILED
      → on SUCCESS + recurrence: spawn child task

**Async webhook** (executor returns 202 + check_url):

    execute_task
      → mark RUNNING
      → POST webhook (retries as above)
      → save check_url, stay RUNNING, register poll job

    poll_task  (separate APScheduler job)
      → GET check_url (with retries + POLL audit rows)
      → mark SUCCESS or FAILED when executor status is terminal
      → on SUCCESS + recurrence: spawn child task

**Transactions**

Each DB touch uses a *short* session (commit, close). Connections are not held
open while tenacity sleeps between retries. Attempt rows are committed even when
we raise a retry signal so the audit trail survives backoff.

**Tenacity signals** (internal, not real HTTP errors)

- ``_RetryableHTTP`` — webhook POST failed or returned 5xx/408/429; retry.
- ``_StillPending`` — poll GET succeeded but executor status not terminal; retry.
"""

import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, TypeVar

import httpx
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.models.task import Task, TaskAttempt
from app.repositories.task_repository import TaskRepository
from app.utils.config import settings
from app.utils.enums import AttemptPhase, TaskStatus
from app.utils.http_client import get_http_client
from app.utils.logging import get_logger

log = get_logger("execution")

T = TypeVar("T")
_TERMINAL = {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED}


class _RetryableHTTP(Exception):
    """Tell tenacity to back off and retry the webhook POST."""


class _StillPending(Exception):
    """Tell tenacity to back off and poll check_url again."""


# ---------------------------------------------------------------------------
# Public entry points (APScheduler callbacks)
# ---------------------------------------------------------------------------


async def execute_task(task_id: str, session_factory) -> None:
    """Fire the task webhook and move the task to a terminal or async-pending state."""
    max_retries = await _run_in_session(session_factory, _mark_running, task_id)
    if not max_retries:
        return  # missing or already terminal

    resp = await _post_webhook_with_retries(session_factory, task_id, max_retries)
    if resp is None:
        return  # retries exhausted → already marked FAILED

    outcome = await _run_in_session(
        session_factory, _apply_execute_result, task_id, resp
    )
    if outcome == "success":
        log.info("task_success", task_id=task_id)


async def poll_task(task_id: str, session_factory) -> None:
    """Follow up a 202 webhook by polling check_url until the executor finishes."""
    try:
        await _poll_until_terminal(session_factory, task_id)
    except (_StillPending, RetryError):
        await _run_in_session(session_factory, _set_status, task_id, TaskStatus.FAILED)
        log.info("poll_terminal", task_id=task_id, status=TaskStatus.FAILED.value)


# ---------------------------------------------------------------------------
# Execute path
# ---------------------------------------------------------------------------


async def _post_webhook_with_retries(
    session_factory, task_id: str, max_retries: int
) -> httpx.Response | None:
    try:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(
                multiplier=settings.backoff_base_seconds,
                max=settings.backoff_max_seconds,
            ),
            retry=retry_if_exception_type(_RetryableHTTP),
            reraise=True,
        ):
            with attempt:
                return await _run_in_session_attempt(
                    session_factory,
                    _execute_one_attempt,
                    task_id,
                    attempt.retry_state.attempt_number,
                )
    except (_RetryableHTTP, RetryError):
        await _run_in_session(session_factory, _set_status, task_id, TaskStatus.FAILED)
        log.warning("task_failed_after_retries", task_id=task_id)
        return None
    return None


async def _execute_one_attempt(
    session, task_id: str, attempt_number: int
) -> httpx.Response:
    repo = TaskRepository(session)
    task = await repo.get(task_id)
    if not task:
        raise _RetryableHTTP()

    task.retry_count = attempt_number - 1
    await repo.update(task)

    row = TaskAttempt(
        task_id=task.id,
        attempt_number=attempt_number,
        phase=AttemptPhase.EXECUTE,
        started_at=_now(),
    )
    t0 = time.perf_counter()
    try:
        resp = await get_http_client().post(task.webhook_url, json=task.payload)
        row.http_status = resp.status_code
        row.response_body = resp.text[:2000]
    except Exception as exc:
        row.error = str(exc)[:2000]
        row.finished_at = _now()
        row.duration_ms = int((time.perf_counter() - t0) * 1000)
        await repo.add_attempt(row)
        raise _RetryableHTTP() from exc

    row.finished_at = _now()
    row.duration_ms = int((time.perf_counter() - t0) * 1000)
    await repo.add_attempt(row)

    if resp.status_code >= 500 or resp.status_code in (408, 429):
        raise _RetryableHTTP()
    return resp


async def _apply_execute_result(session, task_id: str, resp: httpx.Response) -> str:
    repo = TaskRepository(session)
    task = await repo.get(task_id)
    if not task or task.status in _TERMINAL:
        return "skipped"

    if 200 <= resp.status_code < 300 and resp.status_code != 202:
        task.status = TaskStatus.SUCCESS
        await repo.update(task)
        await _schedule_next_occurrence(repo, task)
        return "success"

    if resp.status_code == 202:
        check_url = _parse_check_url(resp, task.id)
        if not check_url:
            task.status = TaskStatus.FAILED
            await repo.update(task)
            return "failed"
        task.check_url = check_url
        task.status = TaskStatus.RUNNING
        await repo.update(task)
        log.info("task_async_accepted", task_id=task.id, check_url=check_url)
        _register_poll_job_safe(task)
        return "async"

    task.status = TaskStatus.FAILED
    await repo.update(task)
    return "failed"


def _parse_check_url(resp: httpx.Response, task_id: str) -> str | None:
    try:
        body = resp.json()
        if isinstance(body, dict) and body.get("check_url"):
            return body["check_url"]
    except Exception as exc:
        log.warning("async_body_parse_failed", task_id=task_id, error=str(exc))
    log.warning("async_missing_check_url", task_id=task_id)
    return None


# ---------------------------------------------------------------------------
# Poll path
# ---------------------------------------------------------------------------


async def _poll_until_terminal(session_factory, task_id: str) -> None:
    n = 0
    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(settings.poll_max_attempts),
        wait=wait_exponential(
            multiplier=settings.poll_interval_seconds,
            max=settings.backoff_max_seconds,
        ),
        retry=retry_if_exception_type(_StillPending),
        reraise=True,
    ):
        with attempt:
            n += 1
            outcome = await _run_in_session_attempt(
                session_factory, _poll_once, task_id, n
            )
            if outcome in {"success", "failed", "skipped"}:
                return


async def _poll_once(session, task_id: str, attempt_number: int) -> str:
    repo = TaskRepository(session)
    task = await repo.get(task_id)
    if not task or not task.check_url or task.status in _TERMINAL:
        return "skipped"

    row = TaskAttempt(
        task_id=task.id,
        attempt_number=attempt_number,
        phase=AttemptPhase.POLL,
        started_at=_now(),
    )
    t0 = time.perf_counter()
    try:
        r = await get_http_client().get(task.check_url)
        row.http_status = r.status_code
        row.response_body = r.text[:2000]
        executor_status = (r.json() or {}).get("status")
    except Exception as exc:
        row.error = str(exc)[:2000]
        row.finished_at = _now()
        row.duration_ms = int((time.perf_counter() - t0) * 1000)
        await repo.add_attempt(row)
        log.warning("poll_attempt_error", task_id=task.id, error=str(exc))
        raise _StillPending() from exc

    row.finished_at = _now()
    row.duration_ms = int((time.perf_counter() - t0) * 1000)
    await repo.add_attempt(row)

    if executor_status == "SUCCESS":
        task.status = TaskStatus.SUCCESS
        await repo.update(task)
        await _schedule_next_occurrence(repo, task)
        log.info("poll_terminal", task_id=task.id, status=TaskStatus.SUCCESS.value)
        return "success"
    if executor_status == "FAILED":
        task.status = TaskStatus.FAILED
        await repo.update(task)
        log.info("poll_terminal", task_id=task.id, status=TaskStatus.FAILED.value)
        return "failed"
    raise _StillPending()


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _run_in_session(
    session_factory, fn: Callable[..., Awaitable[T]], *args: Any
) -> T:
    """Open session → run fn → commit (or rollback on error)."""
    async with session_factory() as session:
        try:
            result = await fn(session, *args)
            await session.commit()
            return result
        except Exception:
            await session.rollback()
            raise


async def _run_in_session_attempt(
    session_factory, fn: Callable[..., Awaitable[T]], *args: Any
) -> T:
    """Like _run_in_session, but commits before re-raising retry signals.

    Without this, a rolled-back attempt would disappear from task_attempts and
    tests/production audits would show gaps between retries.
    """
    async with session_factory() as session:
        try:
            result = await fn(session, *args)
            await session.commit()
            return result
        except (_RetryableHTTP, _StillPending) as exc:
            await session.commit()
            raise exc
        except Exception:
            await session.rollback()
            raise


async def _mark_running(session, task_id: str) -> int:
    """Set RUNNING. Returns max_retries, or 0 if the task should not run."""
    repo = TaskRepository(session)
    task = await repo.get(task_id)
    if not task or task.status in _TERMINAL:
        return 0
    task.status = TaskStatus.RUNNING
    await repo.update(task)
    return task.max_retries


async def _set_status(session, task_id: str, status: TaskStatus) -> None:
    repo = TaskRepository(session)
    task = await repo.get(task_id)
    if not task or task.status in _TERMINAL:
        return
    task.status = status
    await repo.update(task)


async def _schedule_next_occurrence(repo, task: Task) -> None:
    """After SUCCESS, create a child task for the next recurrence slot (if any)."""
    from app.utils.recurrence import next_run_time

    try:
        nxt = next_run_time(task.recurrence, task.execution_time, task.cron_expr)
    except Exception as exc:
        log.warning("recurrence_compute_failed", task_id=task.id, error=str(exc))
        return
    if nxt is None:
        return

    child = Task(
        name=task.name,
        execution_time=nxt,
        webhook_url=task.webhook_url,
        payload=task.payload,
        recurrence=task.recurrence,
        cron_expr=task.cron_expr,
        status=TaskStatus.PENDING,
        max_retries=task.max_retries,
        parent_task_id=task.id,
    )
    await repo.add(child)
    try:
        from app.services.scheduler import register_task_job

        register_task_job(child)
    except Exception as exc:
        log.warning("job_registration_failed", task_id=child.id, error=str(exc))


def _register_poll_job_safe(task: Task) -> None:
    try:
        from app.services.scheduler import register_poll_job

        register_poll_job(task)
    except Exception as exc:
        log.warning("job_registration_failed", task_id=task.id, error=str(exc))
