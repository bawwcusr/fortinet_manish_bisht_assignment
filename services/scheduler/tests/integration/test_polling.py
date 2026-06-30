import respx
from httpx import Response
from datetime import datetime, timezone
from app.models.task import Task
from app.utils.enums import TaskStatus, Recurrence, AttemptPhase
from app.repositories.task_repository import TaskRepository
from app.services.execution import execute_task, poll_task
from tests.integration.test_execution_sync import _factory


@respx.mock
async def test_202_stores_check_url_and_polls_to_success(session, monkeypatch):
    monkeypatch.setattr("app.services.execution.settings.poll_interval_seconds", 0.0)
    repo = TaskRepository(session)
    task = await repo.add(
        Task(
            name="t",
            execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            webhook_url="http://e/daily-summary",
            payload={},
            recurrence=Recurrence.NONE,
            status=TaskStatus.PENDING,
            max_retries=3,
        )
    )
    respx.post("http://e/daily-summary").mock(
        return_value=Response(
            202, json={"status": "QUEUED", "check_url": "http://e/status/xyz"}
        )
    )
    respx.get("http://e/status/xyz").mock(
        side_effect=[
            Response(200, json={"status": "RUNNING"}),
            Response(200, json={"status": "SUCCESS"}),
        ]
    )

    await execute_task(task.id, session_factory=_factory(session))
    refreshed = await repo.get(task.id)
    assert refreshed.check_url == "http://e/status/xyz"
    assert refreshed.status == TaskStatus.RUNNING  # awaiting poll

    await poll_task(task.id, session_factory=_factory(session))
    final = await repo.get(task.id)
    assert final.status == TaskStatus.SUCCESS
    polls = [
        a for a in await repo.attempts_for(task.id) if a.phase == AttemptPhase.POLL
    ]
    assert len(polls) >= 1


@respx.mock
async def test_202_without_check_url_marks_failed(session):
    repo = TaskRepository(session)
    task = await repo.add(
        Task(
            name="t",
            execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            webhook_url="http://e/daily-summary",
            payload={},
            recurrence=Recurrence.NONE,
            status=TaskStatus.PENDING,
            max_retries=3,
        )
    )
    respx.post("http://e/daily-summary").mock(
        return_value=Response(202, json={"status": "QUEUED"})
    )  # no check_url
    await execute_task(task.id, session_factory=_factory(session))
    refreshed = await repo.get(task.id)
    assert refreshed.status == TaskStatus.FAILED


@respx.mock
async def test_poll_retries_on_transient_error_then_succeeds(session, monkeypatch):
    import httpx as _httpx

    monkeypatch.setattr("app.services.execution.settings.poll_interval_seconds", 0.0)
    repo = TaskRepository(session)
    task = await repo.add(
        Task(
            name="t",
            execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            webhook_url="http://e/x",
            payload={},
            recurrence=Recurrence.NONE,
            status=TaskStatus.RUNNING,
            max_retries=3,
            check_url="http://e/status/abc",
        )
    )
    respx.get("http://e/status/abc").mock(
        side_effect=[
            _httpx.ConnectError("boom"),
            Response(200, json={"status": "SUCCESS"}),
        ]
    )

    await poll_task(task.id, session_factory=_factory(session))
    final = await repo.get(task.id)
    assert final.status == TaskStatus.SUCCESS
