import pytest
import pytest_asyncio
from datetime import datetime, timezone
from httpx import AsyncClient, ASGITransport
from app.models.task import Task
from app.utils.enums import TaskStatus, Recurrence
from app.repositories.task_repository import TaskRepository
from app.exceptions import InvalidCancel
from app.services.task_service import TaskService
from app.main import create_app
from app.utils.deps import get_db
from tests.conftest import TEST_DB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


async def test_cancel_pending_sets_cancelled(session):
    repo = TaskRepository(session)
    task = await repo.add(
        Task(
            name="t",
            execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            webhook_url="http://e/x",
            payload={},
            recurrence=Recurrence.NONE,
            status=TaskStatus.PENDING,
            max_retries=3,
        )
    )
    svc = TaskService(session)
    out = await svc.cancel(task.id)
    assert out.status == TaskStatus.CANCELLED


async def test_cannot_cancel_succeeded(session):
    repo = TaskRepository(session)
    task = await repo.add(
        Task(
            name="t",
            execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            webhook_url="http://e/x",
            payload={},
            recurrence=Recurrence.NONE,
            status=TaskStatus.SUCCESS,
            max_retries=3,
        )
    )
    svc = TaskService(session)
    with pytest.raises(InvalidCancel):
        await svc.cancel(task.id)


@pytest_asyncio.fixture
async def client():
    from app.services import scheduler as sched

    sched.init_scheduler(in_memory=True)
    app = create_app(start_scheduler=False)
    engine = create_async_engine(TEST_DB)
    maker = async_sessionmaker(engine, expire_on_commit=False)

    async def _ov():
        async with maker() as s:
            try:
                yield s
                await s.commit()
            except Exception:
                await s.rollback()
                raise

    app.dependency_overrides[get_db] = _ov
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        yield c
    await engine.dispose()


async def test_cancel_endpoint_then_409_on_second(client):
    body = {
        "name": "c",
        "execution_time": "2030-01-01T10:30:00Z",
        "webhook_url": "http://e/x",
        "payload": {},
        "recurrence": "NONE",
    }
    tid = (await client.post("/tasks", json=body)).json()["id"]
    r1 = await client.delete(f"/tasks/{tid}")
    assert r1.status_code == 200
    assert r1.json()["status"] == "CANCELLED"
    r2 = await client.delete(f"/tasks/{tid}")
    assert r2.status_code == 409


async def test_cancel_missing_returns_404(client):
    r = await client.delete("/tasks/nope")
    assert r.status_code == 404
