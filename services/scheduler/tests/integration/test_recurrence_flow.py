import respx
from httpx import Response
from datetime import datetime, timezone
from app.models.task import Task
from app.utils.enums import TaskStatus, Recurrence
from app.repositories.task_repository import TaskRepository
from app.services.execution import execute_task
from tests.integration.test_execution_sync import _factory


@respx.mock
async def test_daily_task_creates_child_on_success(session):
    repo = TaskRepository(session)
    base = datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)
    task = await repo.add(
        Task(
            name="daily",
            execution_time=base,
            webhook_url="http://e/daily-summary",
            payload={},
            recurrence=Recurrence.DAILY,
            status=TaskStatus.PENDING,
            max_retries=3,
        )
    )
    respx.post("http://e/daily-summary").mock(
        return_value=Response(200, json={"ok": 1})
    )

    await execute_task(task.id, session_factory=_factory(session))

    children = [t for t in await repo.list() if t.parent_task_id == task.id]
    assert len(children) == 1
    assert children[0].execution_time == datetime(
        2030, 1, 2, 10, 0, tzinfo=timezone.utc
    )
    assert children[0].status == TaskStatus.PENDING
