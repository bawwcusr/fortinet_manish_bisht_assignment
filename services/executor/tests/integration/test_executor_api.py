import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from app.main import create_app
from app.utils.deps import get_db
from tests.conftest import TEST_DB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest_asyncio.fixture
async def client():
    app = create_app()
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


async def test_send_welcome_sync_success(client):
    r = await client.post("/webhooks/send-welcome", json={"email": "a@b.com"})
    assert r.status_code == 200
    assert r.json()["status"] == "SUCCESS"


async def test_sync_failed_endpoint_returns_500(client, monkeypatch):
    monkeypatch.setattr(
        "app.services.execution_service.settings.fail_endpoints", "send-welcome"
    )
    r = await client.post("/webhooks/send-welcome", json={"email": "a@b.com"})
    assert r.status_code == 500
    assert r.json()["status"] == "FAILED"


async def test_daily_summary_async_returns_202_and_check_url(client):
    r = await client.post("/webhooks/daily-summary", json={"report": "x"})
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "QUEUED"
    assert "/status/" in body["check_url"]

    tid = body["check_url"].rsplit("/", 1)[-1]
    s = await client.get(f"/status/{tid}")
    assert s.status_code == 200
    assert s.json()["status"] in {"QUEUED", "RUNNING", "SUCCESS"}


async def test_unknown_webhook_returns_404(client):
    r = await client.post("/webhooks/no-such-hook", json={})
    assert r.status_code == 404


async def test_list_webhooks_returns_registry(client):
    r = await client.get("/webhooks")
    assert r.status_code == 200
    names = r.json()["webhooks"]
    assert "send-welcome" in names
    assert names["daily-summary"]["mode"] == "async"
