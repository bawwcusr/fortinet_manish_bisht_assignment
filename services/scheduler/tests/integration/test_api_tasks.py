import pytest_asyncio
import respx
from httpx import AsyncClient, ASGITransport, Response
from app.main import create_app
from app.utils.deps import get_db
from tests.conftest import TEST_DB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest_asyncio.fixture
async def client():
    from app.services import scheduler as sched

    sched.init_scheduler(in_memory=True)
    app = create_app(start_scheduler=False)
    engine = create_async_engine(TEST_DB)
    maker = async_sessionmaker(engine, expire_on_commit=False)

    async def _override():
        async with maker() as s:
            try:
                yield s
                await s.commit()
            except Exception:
                await s.rollback()
                raise

    app.dependency_overrides[get_db] = _override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    await engine.dispose()


async def test_create_and_get_task(client):
    body = {
        "name": "Send Welcome Email",
        "execution_time": "2030-01-01T10:30:00Z",
        "webhook_url": "http://localhost:8081/webhooks/send-welcome",
        "payload": {"email": "a@b.com"},
        "recurrence": "NONE",
    }
    r = await client.post("/tasks", json=body)
    assert r.status_code == 201
    tid = r.json()["id"]
    assert r.json()["status"] == "PENDING"

    g = await client.get(f"/tasks/{tid}")
    assert g.status_code == 200
    assert g.json()["name"] == "Send Welcome Email"


async def test_get_missing_returns_404(client):
    r = await client.get("/tasks/does-not-exist")
    assert r.status_code == 404


async def test_create_returns_503_when_job_registration_fails(client, monkeypatch):
    def _boom(_task):
        raise RuntimeError("scheduler unavailable")

    monkeypatch.setattr("app.services.task_service.register_task_job", _boom)
    body = {
        "name": "orphan",
        "execution_time": "2030-01-01T10:30:00Z",
        "webhook_url": "http://localhost:8081/webhooks/send-welcome",
        "payload": {},
        "recurrence": "NONE",
    }
    r = await client.post("/tasks", json=body)
    assert r.status_code == 503
    assert r.json()["detail"] == "failed to schedule task"
    listed = await client.get("/tasks")
    assert not any(t["name"] == "orphan" for t in listed.json())


@respx.mock
async def test_create_rejects_unknown_webhook(client, monkeypatch):
    monkeypatch.setattr(
        "app.utils.webhook_validation.settings.executor_base_url",
        "http://localhost:8081",
    )
    from app.utils import webhook_validation

    webhook_validation._CACHE["webhooks"] = None
    webhook_validation._CACHE["expires"] = 0.0
    respx.get("http://localhost:8081/webhooks").mock(
        return_value=Response(
            200, json={"webhooks": {"send-welcome": {"mode": "sync"}}}
        )
    )
    body = {
        "name": "bad hook",
        "execution_time": "2030-01-01T10:30:00Z",
        "webhook_url": "http://localhost:8081/webhooks/unknown",
        "payload": {},
        "recurrence": "NONE",
    }
    r = await client.post("/tasks", json=body)
    assert r.status_code == 422
    assert "unknown webhook" in r.json()["detail"]
