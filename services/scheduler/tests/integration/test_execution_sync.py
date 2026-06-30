import respx
from httpx import Response
from datetime import datetime, timezone
from app.models.task import Task
from app.utils.enums import TaskStatus, Recurrence
from app.repositories.task_repository import TaskRepository
from app.services.execution import execute_task


@respx.mock
async def test_sync_2xx_marks_success_and_logs_attempt(session):
    repo = TaskRepository(session)
    task = await repo.add(
        Task(
            name="t",
            execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            webhook_url="http://executor/send-welcome",
            payload={"x": 1},
            recurrence=Recurrence.NONE,
            status=TaskStatus.PENDING,
            max_retries=3,
        )
    )
    route = respx.post("http://executor/send-welcome").mock(
        return_value=Response(200, json={"status": "SUCCESS"})
    )

    await execute_task(task.id, session_factory=_factory(session))

    assert route.called
    refreshed = await repo.get(task.id)
    assert refreshed.status == TaskStatus.SUCCESS
    attempts = await repo.attempts_for(task.id)
    assert len(attempts) == 1
    assert attempts[0].http_status == 200


def _factory(session):
    # returns an async context manager yielding the SAME test session
    class _F:
        def __call__(self):
            class _Ctx:
                async def __aenter__(self_):
                    return session

                async def __aexit__(self_, *a):
                    return False

            return _Ctx()

    return _F()
