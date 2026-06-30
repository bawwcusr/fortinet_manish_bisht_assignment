from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "sqlite+aiosqlite:///./scheduler.db"
    # Sync driver URL for APScheduler jobstore (and Alembic). When unset, derived
    # from database_url — see sync_database_url.
    alembic_database_url: str | None = None
    log_level: str = "INFO"

    # retry/backoff (tenacity). NOTE: max_retries is the TOTAL attempt cap
    # (used with tenacity stop_after_attempt), i.e. the maximum number of
    # webhook attempts, not additional retries beyond the first.
    max_retries: int = 3
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    http_timeout_seconds: float = 10.0

    # polling
    poll_max_attempts: int = 10  # max poll rounds before marking task FAILED
    poll_interval_seconds: float = 5.0  # delay before first poll + backoff base

    # scheduler
    scheduler_misfire_grace_seconds: int = 30

    # When set, POST /tasks rejects webhook_url not in executor's GET /webhooks registry.
    executor_base_url: str | None = None
    webhook_validation_timeout: float = 2.0

    # Postgres/asyncpg pool (ignored for sqlite tests).
    db_pool_size: int = 20
    db_max_overflow: int = 10
    db_pool_pre_ping: bool = True

    @property
    def sync_database_url(self) -> str:
        """Sync SQLAlchemy URL for APScheduler jobstore.

        Prefer explicit ALEMBIC_DATABASE_URL so any sync driver works in prod.
        Falls back to stripping common async dialect suffixes from DATABASE_URL
        for local sqlite/postgres dev only.
        """
        if self.alembic_database_url:
            return self.alembic_database_url
        return self.database_url.replace("+asyncpg", "+psycopg2").replace(
            "+aiosqlite", ""
        )


settings = Settings()
