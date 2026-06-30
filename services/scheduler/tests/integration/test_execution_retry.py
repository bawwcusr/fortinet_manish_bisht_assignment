import respx
from httpx import Response
from datetime import datetime, timezone
from app.models.task import Task
from app.utils.enums import TaskStatus, Recurrence
from app.repositories.task_repository import TaskRepository
from app.services.execution import execute_task
from tests.integration.test_execution_sync import _factory


@respx.mock
async def test_retries_then_succeeds(session, monkeypatch):
    monkeypatch.setattr("app.services.execution.settings.backoff_base_seconds", 0.0)
    monkeypatch.setattr("app.services.execution.settings.backoff_max_seconds", 0.0)
    repo = TaskRepository(session)
    task = await repo.add(
        Task(
            name="t",
            execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            webhook_url="http://e/retry",
            payload={},
            recurrence=Recurrence.NONE,
            status=TaskStatus.PENDING,
            max_retries=3,
        )
    )
    route = respx.post("http://e/retry").mock(
        side_effect=[Response(500), Response(500), Response(200, json={"ok": True})]
    )

    await execute_task(task.id, session_factory=_factory(session))

    assert route.call_count == 3
    refreshed = await repo.get(task.id)
    assert refreshed.status == TaskStatus.SUCCESS
    attempts = await repo.attempts_for(task.id)
    assert len(attempts) == 3


@respx.mock
async def test_exhausts_retries_marks_failed(session, monkeypatch):
    monkeypatch.setattr("app.services.execution.settings.backoff_base_seconds", 0.0)
    monkeypatch.setattr("app.services.execution.settings.backoff_max_seconds", 0.0)
    repo = TaskRepository(session)
    task = await repo.add(
        Task(
            name="t",
            execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            webhook_url="http://e/fail",
            payload={},
            recurrence=Recurrence.NONE,
            status=TaskStatus.PENDING,
            max_retries=2,
        )
    )
    respx.post("http://e/fail").mock(return_value=Response(500))

    await execute_task(task.id, session_factory=_factory(session))

    refreshed = await repo.get(task.id)
    assert refreshed.status == TaskStatus.FAILED
    attempts = await repo.attempts_for(task.id)
    assert len(attempts) == 2  # max_retries attempts
