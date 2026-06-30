import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


TEST_DB = "sqlite+aiosqlite:///./_test.db"
SYNC_TEST_DB = "sqlite:///./_test.db"


@pytest.fixture(scope="session", autouse=True)
def _migrate():
    import os

    if os.path.exists("_test.db"):
        os.remove("_test.db")
    cfg = Config("alembic.ini")
    cfg.set_main_option("script_location", "migrations")
    os.environ["ALEMBIC_DATABASE_URL"] = SYNC_TEST_DB
    command.upgrade(cfg, "head")
    yield
    if os.path.exists("_test.db"):
        os.remove("_test.db")


@pytest.fixture(autouse=True)
def _zero_simulated_delays(monkeypatch):
    """Keep integration tests fast; production defaults simulate real work."""
    monkeypatch.setattr(
        "app.services.execution_service.settings.sync_delay_seconds", 0.0
    )
    monkeypatch.setattr(
        "app.services.execution_service.settings.async_delay_seconds", 0.0
    )


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine(TEST_DB)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        try:
            yield s
            await s.commit()
        except Exception:
            await s.rollback()
            raise
    await engine.dispose()
