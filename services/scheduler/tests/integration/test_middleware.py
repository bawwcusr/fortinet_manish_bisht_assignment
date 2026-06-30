import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import create_app


@pytest_asyncio.fixture
async def client():
    app = create_app(start_scheduler=False)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_health_includes_generated_request_id(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.headers.get("x-request-id")


async def test_request_id_echoed_when_provided(client):
    rid = "custom-request-id-abc"
    r = await client.get("/health", headers={"X-Request-ID": rid})
    assert r.headers.get("x-request-id") == rid
