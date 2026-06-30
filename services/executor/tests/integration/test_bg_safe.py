import pytest
from app.services.execution_service import complete_after_delay


class _BoomSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, *a, **k):
        raise RuntimeError("db boom")

    # ExecutionRepository calls session.get(...); other methods unused here


def _boom_factory():
    return _BoomSession()


async def test_complete_after_delay_swallows_errors(monkeypatch):
    # make the sleep instant
    monkeypatch.setattr(
        "app.services.execution_service.settings.async_delay_seconds", 0.0
    )
    # Must not raise even though the session.get blows up
    await complete_after_delay(_boom_factory, "missing-id", "daily-summary")
