from app.utils.config import Settings


def test_settings_defaults(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    s = Settings()
    assert s.max_retries == 3
    assert s.backoff_base_seconds == 1.0
    assert s.poll_max_attempts >= 1


def test_sync_database_url_prefers_alembic(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@h/db")
    monkeypatch.setenv("ALEMBIC_DATABASE_URL", "postgresql+psycopg2://u:p@h/db")
    assert Settings().sync_database_url == "postgresql+psycopg2://u:p@h/db"


def test_sync_database_url_derives_from_async_when_alembic_unset(monkeypatch):
    monkeypatch.delenv("ALEMBIC_DATABASE_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@h/db")
    assert Settings().sync_database_url == "postgresql+psycopg2://u:p@h/db"
