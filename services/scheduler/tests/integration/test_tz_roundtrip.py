from datetime import datetime, timezone
from app.models.task import Task
from app.utils.enums import TaskStatus, Recurrence
from app.repositories.task_repository import TaskRepository


async def test_execution_time_roundtrips_as_aware_utc(session):
    repo = TaskRepository(session)
    aware = datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)
    task = await repo.add(
        Task(
            name="t",
            execution_time=aware,
            webhook_url="http://e/x",
            payload={},
            recurrence=Recurrence.NONE,
            status=TaskStatus.PENDING,
            max_retries=3,
        )
    )
    task_id = task.id
    # force a fresh read from the DB
    session.expire_all()
    fetched = await repo.get(task_id)
    assert fetched.execution_time.tzinfo is not None
    assert fetched.execution_time == aware


async def test_list_due_accepts_aware_now(session):
    repo = TaskRepository(session)
    past = datetime(2020, 1, 1, tzinfo=timezone.utc)
    await repo.add(
        Task(
            name="t",
            execution_time=past,
            webhook_url="http://e/x",
            payload={},
            recurrence=Recurrence.NONE,
            status=TaskStatus.PENDING,
            max_retries=3,
        )
    )
    due = await repo.list_due(datetime.now(timezone.utc))  # aware now
    assert len(due) >= 1
