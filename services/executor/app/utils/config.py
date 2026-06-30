from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "sqlite+aiosqlite:///./executor.db"
    log_level: str = "INFO"
    sync_delay_seconds: float = 5.0  # simulated downstream business-logic runtime (sync webhooks)
    async_delay_seconds: float = 5.0  # simulated downstream business-logic runtime (async webhooks)
    fail_endpoints: str = ""  # comma list to force FAILED (testing)
    async_max_in_flight: int = 32  # cap concurrent async completion tasks
    db_pool_size: int = 20
    db_max_overflow: int = 10
    db_pool_pre_ping: bool = True


settings = Settings()
