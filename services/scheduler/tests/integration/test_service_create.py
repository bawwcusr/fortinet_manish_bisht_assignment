import pytest
from datetime import datetime, timezone
from app.models.schemas import TaskCreate
from app.services.task_service import TaskService
from app.utils.enums import TaskStatus, Recurrence


async def test_create_persists_pending_task(session):
    svc = TaskService(session)
    dto = TaskCreate(
        name="Send Welcome Email",
        execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
        webhook_url="http://localhost:8081/webhooks/send-welcome",
        payload={"email": "a@b.com"},
        recurrence=Recurrence.NONE,
    )
    task = await svc.create(dto)
    assert task.id
    assert task.status == TaskStatus.PENDING

    fetched = await svc.get(task.id)
    assert fetched.name == "Send Welcome Email"


async def test_custom_cron_requires_expr(session):
    svc = TaskService(session)
    with pytest.raises(ValueError):
        TaskCreate(
            name="x",
            execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            webhook_url="http://e/x",
            payload={},
            recurrence=Recurrence.CUSTOM_CRON,  # missing cron_expr
        )
