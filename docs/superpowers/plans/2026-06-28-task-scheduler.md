# Task Automation & Scheduling System — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build two FastAPI microservices — a Scheduler that fires webhooks at a scheduled time with retries, async polling, and recurrence; and an Executor that simulates business webhooks (sync + async) — fully tested (TDD), migrated with Alembic, and runnable via docker-compose.

**Architecture:** `api -> service -> repository -> models` per service. The Scheduler runs APScheduler `AsyncIOScheduler` in-process with a Postgres `SQLAlchemyJobStore`; `DateTrigger` fires a job callback that calls the execution service. Retries/backoff via `tenacity`, recurrence next-time via `croniter`/`timedelta`. Two Postgres databases. Tests run on SQLite for portability; APScheduler is not started in tests (callbacks invoked directly).

**Tech Stack:** Python 3.10+, FastAPI, uv, SQLAlchemy 2.x async (asyncpg/aiosqlite), Alembic, APScheduler, tenacity, croniter, httpx, structlog, pydantic-settings, pytest + pytest-asyncio + respx.

---

## Repository Layout

Two standalone `uv` projects (true microservice isolation; each independently dockerizable).

```
fortinet_scheduler/
├── docker-compose.yml
├── README.md
├── .env.example
├── scripts/seed.py                      # POSTs 4 sample tasks to scheduler API
├── services/
│   ├── scheduler/
│   │   ├── pyproject.toml
│   │   ├── Dockerfile
│   │   ├── entrypoint.sh                 # alembic upgrade head && uvicorn
│   │   ├── alembic.ini
│   │   ├── migrations/{env.py,script.py.mako,versions/}
│   │   ├── app/
│   │   │   ├── __init__.py
│   │   │   ├── config.py                 # pydantic-settings
│   │   │   ├── logging.py                # structlog
│   │   │   ├── enums.py                  # TaskStatus, Recurrence, AttemptPhase
│   │   │   ├── db.py                     # async engine + session factory + Base
│   │   │   ├── models.py                 # Task, TaskAttempt
│   │   │   ├── schemas.py                # pydantic request/response
│   │   │   ├── repository.py             # DB layer
│   │   │   ├── transitions.py            # status transition guard
│   │   │   ├── recurrence.py             # next_run_time()
│   │   │   ├── service.py                # TaskService (create/get/list/cancel)
│   │   │   ├── execution.py              # execute_task / poll_task (httpx+tenacity)
│   │   │   ├── scheduler.py              # APScheduler setup + (de)register jobs
│   │   │   ├── deps.py                   # FastAPI dependencies
│   │   │   ├── routes.py                 # API routers
│   │   │   └── main.py                   # app factory + lifespan
│   │   └── tests/{conftest.py,unit/,integration/}
│   └── executor/
│       ├── pyproject.toml
│       ├── Dockerfile
│       ├── entrypoint.sh
│       ├── alembic.ini
│       ├── migrations/{env.py,script.py.mako,versions/}
│       ├── app/
│       │   ├── __init__.py
│       │   ├── config.py
│       │   ├── logging.py
│       │   ├── enums.py                  # ExecutionStatus
│       │   ├── db.py
│       │   ├── models.py                 # Execution
│       │   ├── schemas.py
│       │   ├── repository.py
│       │   ├── service.py                # ExecutionService (sync + async simulate)
│       │   ├── deps.py
│       │   ├── routes.py
│       │   └── main.py
│       └── tests/{conftest.py,unit/,integration/}
└── docs/superpowers/...
```

**Shared boilerplate (config/logging/db) is intentionally duplicated** across the two services to keep them independently buildable — this is acceptable per the design's isolation principle.

---

## Conventions used by every task

- All datetimes are timezone-aware UTC.
- Run tests from a service dir: `cd services/<svc> && uv run pytest <args>`.
- `uv add <pkg>` adds a runtime dep; `uv add --dev <pkg>` adds a dev dep.
- Commit after each green task.

---

## Task 0: Workspace bootstrap (both services)

**Files:**
- Create: `services/scheduler/pyproject.toml`, `services/executor/pyproject.toml`
- Create: `.env.example`, `services/scheduler/app/__init__.py`, `services/executor/app/__init__.py`

- [ ] **Step 1: Init scheduler uv project + deps**

```bash
cd services/scheduler
uv init --name scheduler --python 3.10 --no-readme --bare
uv add fastapi "uvicorn[standard]" "sqlalchemy[asyncio]>=2" asyncpg alembic \
       apscheduler tenacity croniter httpx structlog pydantic-settings
uv add --dev pytest pytest-asyncio respx aiosqlite anyio
mkdir -p app tests/unit tests/integration migrations/versions
touch app/__init__.py
```

- [ ] **Step 2: Init executor uv project + deps**

```bash
cd ../executor
uv init --name executor --python 3.10 --no-readme --bare
uv add fastapi "uvicorn[standard]" "sqlalchemy[asyncio]>=2" asyncpg alembic \
       httpx structlog pydantic-settings
uv add --dev pytest pytest-asyncio aiosqlite anyio
mkdir -p app tests/unit tests/integration migrations/versions
touch app/__init__.py
```

- [ ] **Step 3: Configure pytest-asyncio (both services)**

Append to BOTH `services/scheduler/pyproject.toml` and `services/executor/pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 4: Verify both envs resolve**

Run: `cd services/scheduler && uv run python -c "import fastapi, apscheduler, tenacity, croniter; print('ok')"`
Expected: `ok`
Run: `cd services/executor && uv run python -c "import fastapi, sqlalchemy; print('ok')"`
Expected: `ok`

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "chore: bootstrap scheduler and executor uv projects"
```

---

# SLICE 1 — Scheduler core (create + read tasks)

## Task 1.1: Enums + status transition guard (scheduler)

**Files:**
- Create: `services/scheduler/app/enums.py`, `services/scheduler/app/transitions.py`
- Test: `services/scheduler/tests/unit/test_transitions.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_transitions.py
import pytest
from app.enums import TaskStatus
from app.transitions import can_transition, assert_transition, InvalidTransition


def test_allows_pending_to_running():
    assert can_transition(TaskStatus.PENDING, TaskStatus.RUNNING) is True


def test_disallows_success_to_running():
    assert can_transition(TaskStatus.SUCCESS, TaskStatus.RUNNING) is False


def test_assert_transition_raises_on_invalid():
    with pytest.raises(InvalidTransition):
        assert_transition(TaskStatus.SUCCESS, TaskStatus.PENDING)
```

- [ ] **Step 2: Run, expect fail** — `uv run pytest tests/unit/test_transitions.py -v` → ImportError.

- [ ] **Step 3: Implement**

```python
# app/enums.py
from enum import Enum


class TaskStatus(str, Enum):
    CREATED = "CREATED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class Recurrence(str, Enum):
    NONE = "NONE"
    HOURLY = "HOURLY"
    DAILY = "DAILY"
    CUSTOM_CRON = "CUSTOM_CRON"


class AttemptPhase(str, Enum):
    EXECUTE = "EXECUTE"
    POLL = "POLL"
```

```python
# app/transitions.py
from app.enums import TaskStatus

_ALLOWED = {
    TaskStatus.CREATED: {TaskStatus.PENDING, TaskStatus.CANCELLED},
    TaskStatus.PENDING: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.RUNNING},
    TaskStatus.SUCCESS: set(),
    TaskStatus.FAILED: set(),
    TaskStatus.CANCELLED: set(),
}


class InvalidTransition(Exception):
    pass


def can_transition(src: TaskStatus, dst: TaskStatus) -> bool:
    return dst in _ALLOWED.get(src, set())


def assert_transition(src: TaskStatus, dst: TaskStatus) -> None:
    if not can_transition(src, dst):
        raise InvalidTransition(f"{src} -> {dst} not allowed")
```

- [ ] **Step 4: Run, expect pass** — `uv run pytest tests/unit/test_transitions.py -v`.

- [ ] **Step 5: Commit** — `git commit -am "feat(scheduler): task status enums + transition guard"`.

## Task 1.2: Config + logging + db base (scheduler)

**Files:**
- Create: `app/config.py`, `app/logging.py`, `app/db.py`
- Test: `tests/unit/test_config.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_config.py
from app.config import Settings


def test_settings_defaults(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    s = Settings()
    assert s.max_retries == 3
    assert s.backoff_base_seconds == 1.0
    assert s.poll_max_attempts >= 1
```

- [ ] **Step 2: Run, expect fail** — ImportError.

- [ ] **Step 3: Implement**

```python
# app/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "sqlite+aiosqlite:///./scheduler.db"
    log_level: str = "INFO"

    # retry/backoff (tenacity)
    max_retries: int = 3
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    http_timeout_seconds: float = 10.0

    # polling
    poll_max_attempts: int = 10
    poll_interval_seconds: float = 5.0

    # scheduler
    scheduler_misfire_grace_seconds: int = 30


settings = Settings()
```

```python
# app/logging.py
import logging
import structlog
from app.config import settings


def configure_logging() -> None:
    logging.basicConfig(format="%(message)s", level=settings.log_level)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level)
        ),
    )


def get_logger(name: str | None = None):
    return structlog.get_logger(name)
```

```python
# app/db.py
from collections.abc import AsyncIterator
from sqlalchemy.ext.asyncio import (
    AsyncSession, async_sessionmaker, create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase
from app.config import settings


class Base(DeclarativeBase):
    pass


engine = create_async_engine(settings.database_url, future=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session
```

- [ ] **Step 4: Run, expect pass**.
- [ ] **Step 5: Commit** — `git commit -am "feat(scheduler): config, structured logging, async db base"`.

## Task 1.3: Models (scheduler)

**Files:** Create `app/models.py`. Test `tests/unit/test_models.py`.

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_models.py
from app.models import Task, TaskAttempt
from app.enums import TaskStatus, Recurrence


def test_task_table_columns():
    cols = set(Task.__table__.columns.keys())
    assert {"id", "name", "execution_time", "webhook_url", "payload",
            "recurrence", "cron_expr", "status", "max_retries",
            "retry_count", "check_url", "parent_task_id",
            "created_at", "updated_at"} <= cols


def test_attempt_table_columns():
    cols = set(TaskAttempt.__table__.columns.keys())
    assert {"id", "task_id", "attempt_number", "phase", "started_at",
            "finished_at", "duration_ms", "http_status",
            "response_body", "error"} <= cols
```

- [ ] **Step 2: Run, expect fail**.

- [ ] **Step 3: Implement**

```python
# app/models.py
import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    JSON, DateTime, ForeignKey, Integer, String, Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db import Base
from app.enums import TaskStatus, Recurrence, AttemptPhase


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255))
    execution_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    webhook_url: Mapped[str] = mapped_column(String(1024))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    recurrence: Mapped[Recurrence] = mapped_column(String(16), default=Recurrence.NONE)
    cron_expr: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[TaskStatus] = mapped_column(String(16), default=TaskStatus.CREATED)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    check_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    parent_task_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now
    )

    attempts: Mapped[list["TaskAttempt"]] = relationship(
        back_populates="task", cascade="all, delete-orphan"
    )


class TaskAttempt(Base):
    __tablename__ = "task_attempts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"))
    attempt_number: Mapped[int] = mapped_column(Integer)
    phase: Mapped[AttemptPhase] = mapped_column(String(16), default=AttemptPhase.EXECUTE)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    task: Mapped["Task"] = relationship(back_populates="attempts")
```

- [ ] **Step 4: Run, expect pass**.
- [ ] **Step 5: Commit** — `git commit -am "feat(scheduler): Task and TaskAttempt models"`.

## Task 1.4: Alembic setup + initial migration (scheduler)

**Files:** Create `alembic.ini`, `migrations/env.py`, `migrations/script.py.mako`, `entrypoint.sh`.

- [ ] **Step 1: Scaffold alembic** — `uv run alembic init -t async migrations` (creates `alembic.ini` + `migrations/`).

- [ ] **Step 2: Point alembic at sync URL + metadata.** Replace `migrations/env.py` body so it imports models and uses a config-driven URL. Key parts:

```python
# migrations/env.py  (essential edits)
import os
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
from app.db import Base
from app import models  # noqa: F401  (register tables)

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)

# Alembic uses a SYNC driver; translate the async URL.
url = os.getenv("ALEMBIC_DATABASE_URL") or os.getenv("DATABASE_URL", "sqlite:///./scheduler.db")
url = url.replace("+asyncpg", "+psycopg2").replace("+aiosqlite", "")
config.set_main_option("sqlalchemy.url", url)
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.", poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata,
                          render_as_batch=True)  # batch => SQLite-safe
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

Add sync drivers for alembic: `uv add --dev psycopg2-binary` (used only by alembic; prod compose installs it too — add as runtime: `uv add psycopg2-binary`).

- [ ] **Step 3: Autogenerate initial migration**

```bash
ALEMBIC_DATABASE_URL="sqlite:///./_tmp_autogen.db" uv run alembic revision --autogenerate -m "initial tasks schema"
rm -f _tmp_autogen.db
```

Open the generated file in `migrations/versions/` and confirm it creates `tasks` and `task_attempts`.

- [ ] **Step 4: Verify upgrade works on sqlite**

```bash
ALEMBIC_DATABASE_URL="sqlite:///./_verify.db" uv run alembic upgrade head && rm -f _verify.db
```
Expected: no error; "Running upgrade".

- [ ] **Step 5: Create entrypoint**

```bash
# services/scheduler/entrypoint.sh
#!/usr/bin/env sh
set -e
alembic upgrade head
exec uvicorn app.main:app --host 0.0.0.0 --port 8080
```
`chmod +x services/scheduler/entrypoint.sh`

- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat(scheduler): alembic env + initial migration + entrypoint"`.

## Task 1.5: conftest with migrated SQLite test DB (scheduler)

**Files:** Create `tests/conftest.py`.

- [ ] **Step 1: Implement fixtures** (no test-first needed; this is test infra — it will be exercised by 1.6's failing test)

```python
# tests/conftest.py
import asyncio
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


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine(TEST_DB)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
        await s.rollback()
    await engine.dispose()
```

- [ ] **Step 2: Commit** — `git add -A && git commit -m "test(scheduler): migrated sqlite test fixtures"`.

## Task 1.6: Repository + schemas + service (create/get) (scheduler)

**Files:** Create `app/repository.py`, `app/schemas.py`, `app/service.py`. Test `tests/integration/test_service_create.py`.

- [ ] **Step 1: Write failing test**

```python
# tests/integration/test_service_create.py
import pytest
from datetime import datetime, timezone
from app.schemas import TaskCreate
from app.service import TaskService
from app.enums import TaskStatus, Recurrence


async def test_create_persists_pending_task(session):
    svc = TaskService(session)
    dto = TaskCreate(
        name="Send Welcome Email",
        execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
        webhook_url="http://executor:8081/send-welcome",
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
```

- [ ] **Step 2: Run, expect fail**.

- [ ] **Step 3: Implement schemas**

```python
# app/schemas.py
from datetime import datetime
from pydantic import BaseModel, Field, model_validator
from app.enums import TaskStatus, Recurrence, AttemptPhase


class TaskCreate(BaseModel):
    name: str
    execution_time: datetime
    webhook_url: str
    payload: dict = Field(default_factory=dict)
    recurrence: Recurrence = Recurrence.NONE
    cron_expr: str | None = None
    max_retries: int | None = None

    @model_validator(mode="after")
    def _check_cron(self):
        if self.recurrence == Recurrence.CUSTOM_CRON and not self.cron_expr:
            raise ValueError("cron_expr required when recurrence=CUSTOM_CRON")
        return self


class TaskRead(BaseModel):
    id: str
    name: str
    execution_time: datetime
    webhook_url: str
    payload: dict
    recurrence: Recurrence
    cron_expr: str | None
    status: TaskStatus
    max_retries: int
    retry_count: int
    check_url: str | None
    parent_task_id: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class AttemptRead(BaseModel):
    id: str
    attempt_number: int
    phase: AttemptPhase
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int | None
    http_status: int | None
    response_body: str | None
    error: str | None
    model_config = {"from_attributes": True}
```

- [ ] **Step 4: Implement repository**

```python
# app/repository.py
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import Task, TaskAttempt
from app.enums import TaskStatus


class TaskRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(self, task: Task) -> Task:
        self.session.add(task)
        await self.session.commit()
        await self.session.refresh(task)
        return task

    async def get(self, task_id: str) -> Task | None:
        return await self.session.get(Task, task_id)

    async def list(self, status: TaskStatus | None = None, limit: int = 100, offset: int = 0):
        stmt = select(Task).order_by(Task.created_at.desc()).limit(limit).offset(offset)
        if status:
            stmt = stmt.where(Task.status == status)
        return list((await self.session.scalars(stmt)).all())

    async def list_due(self, now):
        stmt = select(Task).where(Task.status == TaskStatus.PENDING,
                                  Task.execution_time <= now)
        return list((await self.session.scalars(stmt)).all())

    async def update(self, task: Task) -> Task:
        await self.session.commit()
        await self.session.refresh(task)
        return task

    async def add_attempt(self, attempt: TaskAttempt) -> TaskAttempt:
        self.session.add(attempt)
        await self.session.commit()
        await self.session.refresh(attempt)
        return attempt

    async def attempts_for(self, task_id: str):
        from sqlalchemy import select as _select
        stmt = _select(TaskAttempt).where(TaskAttempt.task_id == task_id).order_by(
            TaskAttempt.started_at.asc())
        return list((await self.session.scalars(stmt)).all())
```

- [ ] **Step 5: Implement service (create/get/list)**

```python
# app/service.py
from datetime import datetime, timezone
from app.config import settings
from app.enums import TaskStatus
from app.models import Task
from app.repository import TaskRepository
from app.schemas import TaskCreate
from app.transitions import assert_transition


class TaskNotFound(Exception):
    pass


class TaskService:
    def __init__(self, session):
        self.repo = TaskRepository(session)

    async def create(self, dto: TaskCreate) -> Task:
        task = Task(
            name=dto.name,
            execution_time=dto.execution_time,
            webhook_url=dto.webhook_url,
            payload=dto.payload,
            recurrence=dto.recurrence,
            cron_expr=dto.cron_expr,
            status=TaskStatus.CREATED,
            max_retries=dto.max_retries if dto.max_retries is not None else settings.max_retries,
        )
        # CREATED -> PENDING (ready to be scheduled)
        assert_transition(TaskStatus.CREATED, TaskStatus.PENDING)
        task.status = TaskStatus.PENDING
        return await self.repo.add(task)

    async def get(self, task_id: str) -> Task:
        task = await self.repo.get(task_id)
        if not task:
            raise TaskNotFound(task_id)
        return task

    async def list(self, status=None, limit=100, offset=0):
        return await self.repo.list(status, limit, offset)
```

- [ ] **Step 6: Run, expect pass** — `uv run pytest tests/integration/test_service_create.py -v`.
- [ ] **Step 7: Commit** — `git commit -am "feat(scheduler): task repository, schemas, create/get service"`.

## Task 1.7: API routes for create/get/list + app factory (scheduler)

**Files:** Create `app/deps.py`, `app/routes.py`, `app/main.py`. Test `tests/integration/test_api_tasks.py`.

- [ ] **Step 1: Write failing test**

```python
# tests/integration/test_api_tasks.py
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from app.main import create_app
from app.deps import get_db
from tests.conftest import TEST_DB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest_asyncio.fixture
async def client():
    app = create_app(start_scheduler=False)
    engine = create_async_engine(TEST_DB)
    maker = async_sessionmaker(engine, expire_on_commit=False)

    async def _override():
        async with maker() as s:
            yield s

    app.dependency_overrides[get_db] = _override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    await engine.dispose()


async def test_create_and_get_task(client):
    body = {
        "name": "Send Welcome Email",
        "execution_time": "2030-01-01T10:30:00Z",
        "webhook_url": "http://executor:8081/send-welcome",
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
```

- [ ] **Step 2: Run, expect fail**.

- [ ] **Step 3: Implement deps**

```python
# app/deps.py
from collections.abc import AsyncIterator
from sqlalchemy.ext.asyncio import AsyncSession
from app.db import SessionLocal


async def get_db() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session
```

- [ ] **Step 4: Implement routes** (scheduler registration is wired in Slice 3; here create/get/list only, plus the scheduler hook left as an injected optional callable)

```python
# app/routes.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.deps import get_db
from app.enums import TaskStatus
from app.schemas import TaskCreate, TaskRead, AttemptRead
from app.service import TaskService, TaskNotFound

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/tasks", response_model=TaskRead, status_code=201)
async def create_task(dto: TaskCreate, db: AsyncSession = Depends(get_db)):
    svc = TaskService(db)
    task = await svc.create(dto)
    # Slice 3 wires APScheduler registration here.
    from app.scheduler import register_task_job
    register_task_job(task)
    return task


@router.get("/tasks", response_model=list[TaskRead])
async def list_tasks(status: TaskStatus | None = None,
                     limit: int = Query(100, le=500), offset: int = 0,
                     db: AsyncSession = Depends(get_db)):
    return await TaskService(db).list(status, limit, offset)


@router.get("/tasks/{task_id}", response_model=TaskRead)
async def get_task(task_id: str, db: AsyncSession = Depends(get_db)):
    try:
        return await TaskService(db).get(task_id)
    except TaskNotFound:
        raise HTTPException(status_code=404, detail="task not found")


@router.get("/tasks/{task_id}/attempts", response_model=list[AttemptRead])
async def get_attempts(task_id: str, db: AsyncSession = Depends(get_db)):
    from app.repository import TaskRepository
    return await TaskRepository(db).attempts_for(task_id)
```

> NOTE: `register_task_job` is created in Slice 3. Until then it would raise ImportError. To keep Slice 1 green, define a **no-op stub** now in `app/scheduler.py`:

```python
# app/scheduler.py  (Slice-1 stub; replaced in Slice 3)
def register_task_job(task) -> None:
    return None
```

- [ ] **Step 5: Implement app factory**

```python
# app/main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.logging import configure_logging
from app.routes import router


def create_app(start_scheduler: bool = True) -> FastAPI:
    configure_logging()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if start_scheduler:
            from app.scheduler import start, shutdown
            start()
            try:
                yield
            finally:
                shutdown()
        else:
            yield

    app = FastAPI(title="Task Scheduler Service", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()
```

> The Slice-1 `scheduler.py` stub must also expose `start`/`shutdown` no-ops:
```python
def start() -> None: ...
def shutdown() -> None: ...
```

- [ ] **Step 6: Run, expect pass** — `uv run pytest tests/integration/test_api_tasks.py -v`.
- [ ] **Step 7: Full suite green** — `uv run pytest -q`.
- [ ] **Step 8: Commit** — `git commit -am "feat(scheduler): tasks API (create/get/list/attempts) + app factory"`.

---

# SLICE 2 — Executor (sync endpoints + persistence)

## Task 2.1: Executor enums, config, logging, db, model

**Files:** `services/executor/app/{enums,config,logging,db,models}.py`. Test `tests/unit/test_models.py`.

- [ ] **Step 1: Write failing test**

```python
# services/executor/tests/unit/test_models.py
from app.models import Execution


def test_execution_columns():
    cols = set(Execution.__table__.columns.keys())
    assert {"id", "endpoint", "payload", "status",
            "created_at", "updated_at", "logs"} <= cols
```

- [ ] **Step 2: Run, expect fail**.

- [ ] **Step 3: Implement** (mirror scheduler's config/logging/db; enums + model below)

```python
# app/enums.py
from enum import Enum


class ExecutionStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
```

```python
# app/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "sqlite+aiosqlite:///./executor.db"
    log_level: str = "INFO"
    async_delay_seconds: float = 2.0          # simulated async processing time
    fail_endpoints: str = ""                   # comma list to force FAILED (testing)


settings = Settings()
```

`app/logging.py` and `app/db.py` are identical to the scheduler versions (copy, adjusting default DB filename via config).

```python
# app/models.py
import uuid
from datetime import datetime, timezone
from sqlalchemy import JSON, DateTime, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from app.db import Base
from app.enums import ExecutionStatus


def _uuid() -> str: return str(uuid.uuid4())
def _now() -> datetime: return datetime.now(timezone.utc)


class Execution(Base):
    __tablename__ = "executions"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    endpoint: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[ExecutionStatus] = mapped_column(String(16), default=ExecutionStatus.QUEUED)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)
    logs: Mapped[str | None] = mapped_column(Text, nullable=True)
```

- [ ] **Step 4: Run, expect pass**.
- [ ] **Step 5: Commit** — `git commit -am "feat(executor): enums, config, db, Execution model"`.

## Task 2.2: Executor Alembic + entrypoint + conftest

Same procedure as Tasks 1.4 + 1.5 (async alembic init, env.py edits importing `app.models`, autogenerate "initial executions schema", entrypoint on port 8081, migrated sqlite conftest).

- [ ] **Step 1:** `uv run alembic init -t async migrations`; edit `migrations/env.py` (same pattern as scheduler, importing `app.models`).
- [ ] **Step 2:** `uv add psycopg2-binary`; autogenerate: `ALEMBIC_DATABASE_URL="sqlite:///./_tmp.db" uv run alembic revision --autogenerate -m "initial executions schema"; rm -f _tmp.db`.
- [ ] **Step 3:** Verify: `ALEMBIC_DATABASE_URL="sqlite:///./_v.db" uv run alembic upgrade head && rm -f _v.db`.
- [ ] **Step 4:** `entrypoint.sh` → `alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8081`; `chmod +x`.
- [ ] **Step 5:** Create `tests/conftest.py` (identical to scheduler 1.5).
- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat(executor): alembic + entrypoint + test fixtures"`.

## Task 2.3: Executor service + sync endpoints + status

**Files:** `app/{repository,schemas,service,deps,routes,main}.py`. Test `tests/integration/test_executor_api.py`.

- [ ] **Step 1: Write failing test**

```python
# tests/integration/test_executor_api.py
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from app.main import create_app
from app.deps import get_db
from tests.conftest import TEST_DB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest_asyncio.fixture
async def client():
    app = create_app()
    engine = create_async_engine(TEST_DB)
    maker = async_sessionmaker(engine, expire_on_commit=False)

    async def _ov():
        async with maker() as s:
            yield s

    app.dependency_overrides[get_db] = _ov
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        yield c
    await engine.dispose()


async def test_send_welcome_sync_success(client):
    r = await client.post("/send-welcome", json={"email": "a@b.com"})
    assert r.status_code == 200
    assert r.json()["status"] == "SUCCESS"


async def test_daily_summary_async_returns_202_and_check_url(client):
    r = await client.post("/daily-summary", json={"report": "x"})
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "QUEUED"
    assert "/status/" in body["check_url"]

    tid = body["check_url"].rsplit("/", 1)[-1]
    s = await client.get(f"/status/{tid}")
    assert s.status_code == 200
    assert s.json()["status"] in {"QUEUED", "RUNNING", "SUCCESS"}
```

- [ ] **Step 2: Run, expect fail**.

- [ ] **Step 3: Implement repository/schemas/service**

```python
# app/repository.py
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import Execution


class ExecutionRepository:
    def __init__(self, session: AsyncSession): self.session = session

    async def add(self, e: Execution) -> Execution:
        self.session.add(e); await self.session.commit()
        await self.session.refresh(e); return e

    async def get(self, eid: str) -> Execution | None:
        return await self.session.get(Execution, eid)

    async def update(self, e: Execution) -> Execution:
        await self.session.commit(); await self.session.refresh(e); return e
```

```python
# app/schemas.py
from datetime import datetime
from pydantic import BaseModel
from app.enums import ExecutionStatus


class ExecutionRead(BaseModel):
    id: str
    endpoint: str
    status: ExecutionStatus
    created_at: datetime
    updated_at: datetime
    model_config = {"from_attributes": True}


class AsyncAccepted(BaseModel):
    status: str = "QUEUED"
    check_url: str
```

```python
# app/service.py
import asyncio
from app.config import settings
from app.enums import ExecutionStatus
from app.models import Execution
from app.repository import ExecutionRepository


class ExecutionService:
    def __init__(self, session):
        self.repo = ExecutionRepository(session)

    async def run_sync(self, endpoint: str, payload: dict) -> Execution:
        e = Execution(endpoint=endpoint, payload=payload, status=ExecutionStatus.RUNNING)
        await self.repo.add(e)
        failed = endpoint in {x.strip() for x in settings.fail_endpoints.split(",") if x}
        e.status = ExecutionStatus.FAILED if failed else ExecutionStatus.SUCCESS
        e.logs = f"sync processed endpoint={endpoint}"
        return await self.repo.update(e)

    async def enqueue_async(self, endpoint: str, payload: dict) -> Execution:
        e = Execution(endpoint=endpoint, payload=payload, status=ExecutionStatus.QUEUED)
        return await self.repo.add(e)

    async def get(self, eid: str) -> Execution | None:
        return await self.repo.get(eid)


async def complete_after_delay(session_factory, execution_id: str, endpoint: str):
    """Background task: simulate async processing then mark terminal."""
    await asyncio.sleep(settings.async_delay_seconds)
    async with session_factory() as s:
        repo = ExecutionRepository(s)
        e = await repo.get(execution_id)
        if not e:
            return
        failed = endpoint in {x.strip() for x in settings.fail_endpoints.split(",") if x}
        e.status = ExecutionStatus.FAILED if failed else ExecutionStatus.SUCCESS
        e.logs = (e.logs or "") + f"; async completed status={e.status}"
        await repo.update(e)
```

- [ ] **Step 4: Implement deps/routes/main**

```python
# app/deps.py  (identical pattern to scheduler)
from collections.abc import AsyncIterator
from sqlalchemy.ext.asyncio import AsyncSession
from app.db import SessionLocal


async def get_db() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as s:
        yield s
```

```python
# app/routes.py
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from app.db import SessionLocal
from app.deps import get_db
from app.schemas import ExecutionRead, AsyncAccepted
from app.service import ExecutionService, complete_after_delay

router = APIRouter()


@router.get("/health")
async def health(): return {"status": "ok"}


async def _sync(endpoint, payload, db):
    e = await ExecutionService(db).run_sync(endpoint, payload)
    return ExecutionRead.model_validate(e)


@router.post("/send-welcome", response_model=ExecutionRead)
async def send_welcome(payload: dict, db: AsyncSession = Depends(get_db)):
    return await _sync("send-welcome", payload, db)


@router.post("/notify-admin", response_model=ExecutionRead)
async def notify_admin(payload: dict, db: AsyncSession = Depends(get_db)):
    return await _sync("notify-admin", payload, db)


async def _async(endpoint, payload, request, bg, db):
    e = await ExecutionService(db).enqueue_async(endpoint, payload)
    bg.add_task(complete_after_delay, SessionLocal, e.id, endpoint)
    base = str(request.base_url).rstrip("/")
    return AsyncAccepted(check_url=f"{base}/status/{e.id}")


@router.post("/daily-summary", status_code=202, response_model=AsyncAccepted)
async def daily_summary(payload: dict, request: Request, bg: BackgroundTasks,
                        db: AsyncSession = Depends(get_db)):
    return await _async("daily-summary", payload, request, bg, db)


@router.post("/security-alert", status_code=202, response_model=AsyncAccepted)
async def security_alert(payload: dict, request: Request, bg: BackgroundTasks,
                         db: AsyncSession = Depends(get_db)):
    return await _async("security-alert", payload, request, bg, db)


@router.get("/status/{execution_id}", response_model=ExecutionRead)
async def status(execution_id: str, db: AsyncSession = Depends(get_db)):
    e = await ExecutionService(db).get(execution_id)
    if not e:
        raise HTTPException(404, "not found")
    return e
```

```python
# app/main.py
from fastapi import FastAPI
from app.logging import configure_logging
from app.routes import router


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="Task Executor Service")
    app.include_router(router)
    return app


app = create_app()
```

- [ ] **Step 5: Run, expect pass**. Note: the async test asserts status in `{QUEUED,RUNNING,SUCCESS}` so the background timing isn't flaky.
- [ ] **Step 6: Commit** — `git commit -am "feat(executor): sync + async endpoints, status polling, persistence"`.

---

# SLICE 3 — Dispatch + sync execution + attempt logging (scheduler)

## Task 3.1: Execution core — call webhook, log attempt, transition (happy path, sync)

**Files:** Create `app/execution.py`. Test `tests/integration/test_execution_sync.py` (uses `respx` to stub httpx).

- [ ] **Step 1: Write failing test**

```python
# tests/integration/test_execution_sync.py
import respx
from httpx import Response
from datetime import datetime, timezone
from app.models import Task
from app.enums import TaskStatus, Recurrence
from app.repository import TaskRepository
from app.execution import execute_task


@respx.mock
async def test_sync_2xx_marks_success_and_logs_attempt(session):
    repo = TaskRepository(session)
    task = await repo.add(Task(
        name="t", execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
        webhook_url="http://executor/send-welcome", payload={"x": 1},
        recurrence=Recurrence.NONE, status=TaskStatus.PENDING, max_retries=3,
    ))
    route = respx.post("http://executor/send-welcome").mock(
        return_value=Response(200, json={"status": "SUCCESS"}))

    await execute_task(task.id, session_factory=_factory(session))

    assert route.called
    refreshed = await repo.get(task.id)
    assert refreshed.status == TaskStatus.SUCCESS
    attempts = await repo.attempts_for(task.id)
    assert len(attempts) == 1
    assert attempts[0].http_status == 200


def _factory(session):
    # returns an async context manager yielding the SAME test session
    class _F:
        def __call__(self):
            class _Ctx:
                async def __aenter__(self_): return session
                async def __aexit__(self_, *a): return False
            return _Ctx()
    return _F()
```

- [ ] **Step 2: Run, expect fail** — ImportError on `app.execution`.

- [ ] **Step 3: Implement execution (sync path + attempt logging; retries added in Slice 4)**

```python
# app/execution.py
import time
from datetime import datetime, timezone
import httpx
from app.config import settings
from app.enums import TaskStatus, AttemptPhase
from app.logging import get_logger
from app.models import Task, TaskAttempt
from app.repository import TaskRepository

log = get_logger("execution")


def _now():
    return datetime.now(timezone.utc)


async def _call_webhook(task: Task) -> httpx.Response:
    async with httpx.AsyncClient(timeout=settings.http_timeout_seconds) as client:
        return await client.post(task.webhook_url, json=task.payload)


async def execute_task(task_id: str, session_factory) -> None:
    async with session_factory() as session:
        repo = TaskRepository(session)
        task = await repo.get(task_id)
        if not task or task.status in {TaskStatus.SUCCESS, TaskStatus.FAILED,
                                       TaskStatus.CANCELLED}:
            return

        task.status = TaskStatus.RUNNING
        await repo.update(task)

        attempt = TaskAttempt(task_id=task.id, attempt_number=task.retry_count + 1,
                              phase=AttemptPhase.EXECUTE, started_at=_now())
        t0 = time.perf_counter()
        try:
            resp = await _call_webhook(task)
            attempt.http_status = resp.status_code
            attempt.response_body = resp.text[:2000]
        except Exception as exc:  # network/timeout
            attempt.error = str(exc)[:2000]
            attempt.finished_at = _now()
            attempt.duration_ms = int((time.perf_counter() - t0) * 1000)
            await repo.add_attempt(attempt)
            task.status = TaskStatus.FAILED  # retries handled in Slice 4
            await repo.update(task)
            return

        attempt.finished_at = _now()
        attempt.duration_ms = int((time.perf_counter() - t0) * 1000)
        await repo.add_attempt(attempt)

        if 200 <= resp.status_code < 300:
            task.status = TaskStatus.SUCCESS
            await repo.update(task)
            log.info("task_success", task_id=task.id, http_status=resp.status_code)
        else:
            task.status = TaskStatus.FAILED
            await repo.update(task)
            log.warning("task_failed", task_id=task.id, http_status=resp.status_code)
```

- [ ] **Step 4: Run, expect pass**.
- [ ] **Step 5: Commit** — `git commit -am "feat(scheduler): sync webhook execution + attempt logging"`.

## Task 3.2: APScheduler wiring (replace stub)

**Files:** Replace `app/scheduler.py`. Test `tests/integration/test_scheduler_register.py`.

- [ ] **Step 1: Write failing test**

```python
# tests/integration/test_scheduler_register.py
from datetime import datetime, timezone
from app.models import Task
from app.enums import TaskStatus, Recurrence
from app import scheduler as sched


def test_register_and_cancel_job_uses_memory_store(monkeypatch):
    sched.init_scheduler(in_memory=True)
    task = Task(id="abc", name="t",
                execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
                webhook_url="http://e/x", payload={}, recurrence=Recurrence.NONE,
                status=TaskStatus.PENDING, max_retries=3)
    sched.register_task_job(task)
    assert sched.get_scheduler().get_job("abc") is not None
    sched.remove_task_job("abc")
    assert sched.get_scheduler().get_job("abc") is None
```

- [ ] **Step 2: Run, expect fail**.

- [ ] **Step 3: Implement scheduler module**

```python
# app/scheduler.py
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.date import DateTrigger
from app.config import settings
from app.db import SessionLocal
from app.logging import get_logger

log = get_logger("scheduler")
_scheduler: AsyncIOScheduler | None = None


def _sync_jobstore_url() -> str:
    return settings.database_url.replace("+asyncpg", "+psycopg2").replace("+aiosqlite", "")


def init_scheduler(in_memory: bool = False) -> AsyncIOScheduler:
    global _scheduler
    if in_memory:
        _scheduler = AsyncIOScheduler()
    else:
        jobstores = {"default": SQLAlchemyJobStore(url=_sync_jobstore_url())}
        _scheduler = AsyncIOScheduler(jobstores=jobstores)
    return _scheduler


def get_scheduler() -> AsyncIOScheduler:
    if _scheduler is None:
        init_scheduler()
    return _scheduler


async def _run_execute(task_id: str) -> None:
    from app.execution import execute_task
    await execute_task(task_id, session_factory=SessionLocal)


def register_task_job(task) -> None:
    get_scheduler().add_job(
        _run_execute, trigger=DateTrigger(run_date=task.execution_time),
        args=[task.id], id=task.id, replace_existing=True,
        misfire_grace_time=settings.scheduler_misfire_grace_seconds,
    )


def remove_task_job(task_id: str) -> None:
    job = get_scheduler().get_job(task_id)
    if job:
        get_scheduler().remove_job(task_id)


def start() -> None:
    get_scheduler().start()
    log.info("scheduler_started")


def shutdown() -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
```

- [ ] **Step 4: Run, expect pass**. Then full suite: `uv run pytest -q`.
- [ ] **Step 5: Commit** — `git commit -am "feat(scheduler): APScheduler DateTrigger job registration"`.

---

# SLICE 4 — Retries with exponential backoff (tenacity)

## Task 4.1: Retrying webhook call with backoff

**Files:** Modify `app/execution.py`. Test `tests/integration/test_execution_retry.py`.

- [ ] **Step 1: Write failing test**

```python
# tests/integration/test_execution_retry.py
import respx
from httpx import Response
from datetime import datetime, timezone
from app.models import Task
from app.enums import TaskStatus, Recurrence
from app.repository import TaskRepository
from app.execution import execute_task
from tests.integration.test_execution_sync import _factory


@respx.mock
async def test_retries_then_succeeds(session, monkeypatch):
    # make backoff instant
    monkeypatch.setattr("app.execution.settings.backoff_base_seconds", 0.0)
    monkeypatch.setattr("app.execution.settings.backoff_max_seconds", 0.0)
    repo = TaskRepository(session)
    task = await repo.add(Task(
        name="t", execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
        webhook_url="http://e/retry", payload={}, recurrence=Recurrence.NONE,
        status=TaskStatus.PENDING, max_retries=3))
    route = respx.post("http://e/retry").mock(side_effect=[
        Response(500), Response(500), Response(200, json={"ok": True})])

    await execute_task(task.id, session_factory=_factory(session))

    assert route.call_count == 3
    refreshed = await repo.get(task.id)
    assert refreshed.status == TaskStatus.SUCCESS
    attempts = await repo.attempts_for(task.id)
    assert len(attempts) == 3


@respx.mock
async def test_exhausts_retries_marks_failed(session, monkeypatch):
    monkeypatch.setattr("app.execution.settings.backoff_base_seconds", 0.0)
    monkeypatch.setattr("app.execution.settings.backoff_max_seconds", 0.0)
    repo = TaskRepository(session)
    task = await repo.add(Task(
        name="t", execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
        webhook_url="http://e/fail", payload={}, recurrence=Recurrence.NONE,
        status=TaskStatus.PENDING, max_retries=2))
    respx.post("http://e/fail").mock(return_value=Response(500))

    await execute_task(task.id, session_factory=_factory(session))

    refreshed = await repo.get(task.id)
    assert refreshed.status == TaskStatus.FAILED
    attempts = await repo.attempts_for(task.id)
    assert len(attempts) == 2  # max_retries attempts
```

- [ ] **Step 2: Run, expect fail** (current code only attempts once).

- [ ] **Step 3: Refactor execution to use tenacity.** Replace the single-call body in `execute_task` with a retry loop that records each attempt. Use tenacity's `AsyncRetrying` so we can log each attempt and treat non-2xx (except 202) as retryable:

```python
# app/execution.py  (additions/replacements)
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_exponential, retry_if_exception_type


class _RetryableHTTP(Exception):
    def __init__(self, response): self.response = response


async def _attempt_once(task, repo, attempt_number):
    attempt = TaskAttempt(task_id=task.id, attempt_number=attempt_number,
                          phase=AttemptPhase.EXECUTE, started_at=_now())
    t0 = time.perf_counter()
    try:
        resp = await _call_webhook(task)
        attempt.http_status = resp.status_code
        attempt.response_body = resp.text[:2000]
    except Exception as exc:
        attempt.error = str(exc)[:2000]
        attempt.finished_at = _now()
        attempt.duration_ms = int((time.perf_counter() - t0) * 1000)
        await repo.add_attempt(attempt)
        raise _RetryableHTTP(None) from exc
    attempt.finished_at = _now()
    attempt.duration_ms = int((time.perf_counter() - t0) * 1000)
    await repo.add_attempt(attempt)
    if resp.status_code >= 500 or resp.status_code in (408, 429):
        raise _RetryableHTTP(resp)
    return resp


async def execute_task(task_id: str, session_factory) -> None:
    async with session_factory() as session:
        repo = TaskRepository(session)
        task = await repo.get(task_id)
        if not task or task.status in {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED}:
            return
        task.status = TaskStatus.RUNNING
        await repo.update(task)

        resp = None
        try:
            async for at in AsyncRetrying(
                stop=stop_after_attempt(task.max_retries),
                wait=wait_exponential(multiplier=settings.backoff_base_seconds,
                                      max=settings.backoff_max_seconds),
                retry=retry_if_exception_type(_RetryableHTTP),
                reraise=True,
            ):
                with at:
                    task.retry_count = at.retry_state.attempt_number - 1
                    resp = await _attempt_once(task, repo, at.retry_state.attempt_number)
        except (_RetryableHTTP, RetryError):
            task.status = TaskStatus.FAILED
            await repo.update(task)
            log.warning("task_failed_after_retries", task_id=task.id)
            return

        # success or terminal non-retryable status
        if resp is not None and 200 <= resp.status_code < 300:
            task.status = TaskStatus.SUCCESS
        elif resp is not None and resp.status_code == 202:
            return  # async path handled in Slice 5
        else:
            task.status = TaskStatus.FAILED
        await repo.update(task)
```

Delete the old single-attempt success/fail tail from Task 3.1 (it is fully replaced).

- [ ] **Step 4: Run, expect pass**. Re-run Slice-3 sync test to ensure still green.
- [ ] **Step 5: Commit** — `git commit -am "feat(scheduler): tenacity exponential-backoff retries"`.

---

# SLICE 5 — Async execution + polling (scheduler)

## Task 5.1: Detect 202 and schedule polling; poll_task reconciles

**Files:** Modify `app/execution.py` (add `poll_task` + 202 handling), `app/scheduler.py` (add `register_poll_job`). Test `tests/integration/test_polling.py`.

- [ ] **Step 1: Write failing test**

```python
# tests/integration/test_polling.py
import respx
from httpx import Response
from datetime import datetime, timezone
from app.models import Task
from app.enums import TaskStatus, Recurrence
from app.repository import TaskRepository
from app.execution import execute_task, poll_task
from tests.integration.test_execution_sync import _factory


@respx.mock
async def test_202_stores_check_url_and_polls_to_success(session, monkeypatch):
    monkeypatch.setattr("app.execution.settings.poll_interval_seconds", 0.0)
    repo = TaskRepository(session)
    task = await repo.add(Task(
        name="t", execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
        webhook_url="http://e/daily-summary", payload={}, recurrence=Recurrence.NONE,
        status=TaskStatus.PENDING, max_retries=3))
    respx.post("http://e/daily-summary").mock(return_value=Response(
        202, json={"status": "QUEUED", "check_url": "http://e/status/xyz"}))
    respx.get("http://e/status/xyz").mock(side_effect=[
        Response(200, json={"status": "RUNNING"}),
        Response(200, json={"status": "SUCCESS"})])

    await execute_task(task.id, session_factory=_factory(session))
    refreshed = await repo.get(task.id)
    assert refreshed.check_url == "http://e/status/xyz"
    assert refreshed.status == TaskStatus.RUNNING  # awaiting poll

    await poll_task(task.id, session_factory=_factory(session))
    final = await repo.get(task.id)
    assert final.status == TaskStatus.SUCCESS
    polls = [a for a in await repo.attempts_for(task.id) if a.phase.value == "POLL"]
    assert len(polls) >= 1
```

- [ ] **Step 2: Run, expect fail**.

- [ ] **Step 3: Implement.** In `execute_task`, replace the `elif resp.status_code == 202` branch to persist `check_url` and register a poll job (only when not in test direct-call — registration is best-effort and guarded):

```python
# app/execution.py  (202 handling)
        if resp is not None and 200 <= resp.status_code < 300 and resp.status_code != 202:
            task.status = TaskStatus.SUCCESS
            await repo.update(task)
        elif resp is not None and resp.status_code == 202:
            data = resp.json()
            task.check_url = data.get("check_url")
            task.status = TaskStatus.RUNNING
            await repo.update(task)
            try:
                from app.scheduler import register_poll_job
                register_poll_job(task)
            except Exception:
                pass  # in tests we call poll_task directly
        else:
            task.status = TaskStatus.FAILED
            await repo.update(task)
```

Add polling with tenacity (re-poll until terminal or cap):

```python
# app/execution.py  (poll_task)
class _StillPending(Exception):
    pass


async def poll_task(task_id: str, session_factory) -> None:
    async with session_factory() as session:
        repo = TaskRepository(session)
        task = await repo.get(task_id)
        if not task or not task.check_url or task.status in {
                TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED}:
            return
        n = 0
        try:
            async for at in AsyncRetrying(
                stop=stop_after_attempt(settings.poll_max_attempts),
                wait=wait_exponential(multiplier=settings.poll_interval_seconds,
                                      max=settings.backoff_max_seconds),
                retry=retry_if_exception_type(_StillPending),
                reraise=True,
            ):
                with at:
                    n += 1
                    pa = TaskAttempt(task_id=task.id, attempt_number=n,
                                     phase=AttemptPhase.POLL, started_at=_now())
                    async with httpx.AsyncClient(timeout=settings.http_timeout_seconds) as c:
                        r = await c.get(task.check_url)
                    pa.http_status = r.status_code
                    pa.response_body = r.text[:2000]
                    pa.finished_at = _now()
                    await repo.add_attempt(pa)
                    status = (r.json() or {}).get("status")
                    if status == "SUCCESS":
                        task.status = TaskStatus.SUCCESS
                        await repo.update(task)
                        return
                    if status == "FAILED":
                        task.status = TaskStatus.FAILED
                        await repo.update(task)
                        return
                    raise _StillPending()
        except (_StillPending, RetryError):
            task.status = TaskStatus.FAILED  # exceeded poll cap
            await repo.update(task)
```

Add to `app/scheduler.py`:

```python
# app/scheduler.py
from datetime import datetime, timedelta, timezone

async def _run_poll(task_id: str) -> None:
    from app.execution import poll_task
    await poll_task(task_id, session_factory=SessionLocal)


def register_poll_job(task) -> None:
    run_at = datetime.now(timezone.utc) + timedelta(seconds=settings.poll_interval_seconds)
    get_scheduler().add_job(
        _run_poll, trigger=DateTrigger(run_date=run_at),
        args=[task.id], id=f"poll:{task.id}", replace_existing=True,
        misfire_grace_time=settings.scheduler_misfire_grace_seconds)
```

- [ ] **Step 4: Run, expect pass**. Full suite green.
- [ ] **Step 5: Commit** — `git commit -am "feat(scheduler): async 202 detection + status polling"`.

---

# SLICE 6 — Recurrence

## Task 6.1: next_run_time helper

**Files:** Create `app/recurrence.py`. Test `tests/unit/test_recurrence.py`.

- [ ] **Step 1: Write failing test**

```python
# tests/unit/test_recurrence.py
from datetime import datetime, timezone, timedelta
from app.enums import Recurrence
from app.recurrence import next_run_time


def test_hourly_adds_one_hour():
    base = datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert next_run_time(Recurrence.HOURLY, base, None) == base + timedelta(hours=1)


def test_daily_adds_one_day():
    base = datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert next_run_time(Recurrence.DAILY, base, None) == base + timedelta(days=1)


def test_custom_cron_uses_croniter():
    base = datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)
    nxt = next_run_time(Recurrence.CUSTOM_CRON, base, "0 12 * * *")
    assert nxt == datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_none_returns_none():
    base = datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert next_run_time(Recurrence.NONE, base, None) is None
```

- [ ] **Step 2: Run, expect fail**.

- [ ] **Step 3: Implement**

```python
# app/recurrence.py
from datetime import datetime, timedelta
from croniter import croniter
from app.enums import Recurrence


def next_run_time(recurrence: Recurrence, base: datetime, cron_expr: str | None):
    if recurrence == Recurrence.HOURLY:
        return base + timedelta(hours=1)
    if recurrence == Recurrence.DAILY:
        return base + timedelta(days=1)
    if recurrence == Recurrence.CUSTOM_CRON:
        return croniter(cron_expr, base).get_next(datetime)
    return None
```

- [ ] **Step 4: Run, expect pass**.
- [ ] **Step 5: Commit** — `git commit -am "feat(scheduler): recurrence next_run_time helper"`.

## Task 6.2: Auto-schedule next occurrence on success

**Files:** Modify `app/execution.py` (on SUCCESS, create child task), `app/service.py` (add `schedule_next`). Test `tests/integration/test_recurrence_flow.py`.

- [ ] **Step 1: Write failing test**

```python
# tests/integration/test_recurrence_flow.py
import respx
from httpx import Response
from datetime import datetime, timezone
from app.models import Task
from app.enums import TaskStatus, Recurrence
from app.repository import TaskRepository
from app.execution import execute_task
from tests.integration.test_execution_sync import _factory


@respx.mock
async def test_daily_task_creates_child_on_success(session):
    repo = TaskRepository(session)
    base = datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)
    task = await repo.add(Task(
        name="daily", execution_time=base, webhook_url="http://e/daily-summary",
        payload={}, recurrence=Recurrence.DAILY, status=TaskStatus.PENDING, max_retries=3))
    respx.post("http://e/daily-summary").mock(return_value=Response(200, json={"ok": 1}))

    await execute_task(task.id, session_factory=_factory(session))

    children = [t for t in await repo.list() if t.parent_task_id == task.id]
    assert len(children) == 1
    assert children[0].execution_time == datetime(2030, 1, 2, 10, 0, tzinfo=timezone.utc)
    assert children[0].status == TaskStatus.PENDING
```

- [ ] **Step 2: Run, expect fail**.

- [ ] **Step 3: Implement.** After setting `task.status = SUCCESS` in `execute_task`, schedule next:

```python
# app/execution.py  (inside execute_task, immediately after a SUCCESS commit)
            from app.recurrence import next_run_time
            nxt = next_run_time(task.recurrence, task.execution_time, task.cron_expr)
            if nxt is not None:
                child = Task(
                    name=task.name, execution_time=nxt, webhook_url=task.webhook_url,
                    payload=task.payload, recurrence=task.recurrence,
                    cron_expr=task.cron_expr, status=TaskStatus.PENDING,
                    max_retries=task.max_retries, parent_task_id=task.id)
                await repo.add(child)
                try:
                    from app.scheduler import register_task_job
                    register_task_job(child)
                except Exception:
                    pass
```

(Apply the same recurrence block in `poll_task` after async SUCCESS so recurring async tasks also re-schedule. Extract a private `_schedule_next(repo, task)` helper to stay DRY and call it from both places.)

```python
# app/execution.py  (DRY helper)
async def _schedule_next(repo, task) -> None:
    from app.recurrence import next_run_time
    nxt = next_run_time(task.recurrence, task.execution_time, task.cron_expr)
    if nxt is None:
        return
    child = Task(name=task.name, execution_time=nxt, webhook_url=task.webhook_url,
                 payload=task.payload, recurrence=task.recurrence, cron_expr=task.cron_expr,
                 status=TaskStatus.PENDING, max_retries=task.max_retries,
                 parent_task_id=task.id)
    await repo.add(child)
    try:
        from app.scheduler import register_task_job
        register_task_job(child)
    except Exception:
        pass
```

Call `await _schedule_next(repo, task)` after SUCCESS in both `execute_task` and `poll_task`.

- [ ] **Step 4: Run, expect pass**. Full suite green.
- [ ] **Step 5: Commit** — `git commit -am "feat(scheduler): auto-schedule next run for recurring tasks"`.

---

# SLICE 7 — Cancellation

## Task 7.1: Cancel service + endpoint

**Files:** Modify `app/service.py` (add `cancel`), `app/routes.py` (add cancel route). Test `tests/integration/test_cancel.py`.

- [ ] **Step 1: Write failing test**

```python
# tests/integration/test_cancel.py
import pytest
from datetime import datetime, timezone
from app.models import Task
from app.enums import TaskStatus, Recurrence
from app.repository import TaskRepository
from app.service import TaskService, InvalidCancel


async def test_cancel_pending_sets_cancelled(session):
    repo = TaskRepository(session)
    task = await repo.add(Task(
        name="t", execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
        webhook_url="http://e/x", payload={}, recurrence=Recurrence.NONE,
        status=TaskStatus.PENDING, max_retries=3))
    svc = TaskService(session)
    out = await svc.cancel(task.id)
    assert out.status == TaskStatus.CANCELLED


async def test_cannot_cancel_succeeded(session):
    repo = TaskRepository(session)
    task = await repo.add(Task(
        name="t", execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
        webhook_url="http://e/x", payload={}, recurrence=Recurrence.NONE,
        status=TaskStatus.SUCCESS, max_retries=3))
    svc = TaskService(session)
    with pytest.raises(InvalidCancel):
        await svc.cancel(task.id)
```

- [ ] **Step 2: Run, expect fail**.

- [ ] **Step 3: Implement service.cancel**

```python
# app/service.py  (additions)
from app.enums import TaskStatus


class InvalidCancel(Exception):
    pass


# inside TaskService:
    async def cancel(self, task_id: str):
        task = await self.get(task_id)
        if task.status in {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED}:
            raise InvalidCancel(task.status)
        task.status = TaskStatus.CANCELLED
        updated = await self.repo.update(task)
        try:
            from app.scheduler import remove_task_job
            remove_task_job(task_id)
        except Exception:
            pass
        return updated
```

- [ ] **Step 4: Add route**

```python
# app/routes.py  (addition)
@router.post("/tasks/{task_id}/cancel", response_model=TaskRead)
async def cancel_task(task_id: str, db: AsyncSession = Depends(get_db)):
    from app.service import InvalidCancel
    svc = TaskService(db)
    try:
        return await svc.cancel(task_id)
    except TaskNotFound:
        raise HTTPException(404, "task not found")
    except InvalidCancel:
        raise HTTPException(409, "task already terminal")
```

- [ ] **Step 5: Run, expect pass**. Full suite green.
- [ ] **Step 6: Commit** — `git commit -am "feat(scheduler): task cancellation"`.

---

# SLICE 8 — Cross-cutting: Docker, seed, README, polish

## Task 8.1: Dockerfiles (both services)

**Files:** Create `services/scheduler/Dockerfile`, `services/executor/Dockerfile`.

- [ ] **Step 1: Write Dockerfile (scheduler; executor identical except port 8081)**

```dockerfile
# services/scheduler/Dockerfile
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev || uv sync --no-dev
COPY . .
RUN chmod +x entrypoint.sh
EXPOSE 8080
CMD ["uv", "run", "./entrypoint.sh"]
```

- [ ] **Step 2: Commit** — `git add -A && git commit -m "build: Dockerfiles for both services"`.

## Task 8.2: docker-compose + .env.example

**Files:** Create `docker-compose.yml`, `.env.example`.

- [ ] **Step 1: Write compose**

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app
      POSTGRES_DB: scheduler_db
    ports: ["5432:5432"]
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./scripts/init-dbs.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app"]
      interval: 5s
      timeout: 3s
      retries: 10

  executor:
    build: ./services/executor
    environment:
      DATABASE_URL: postgresql+asyncpg://app:app@postgres:5432/executor_db
      ALEMBIC_DATABASE_URL: postgresql+psycopg2://app:app@postgres:5432/executor_db
    depends_on:
      postgres: {condition: service_healthy}
    ports: ["8081:8081"]

  scheduler:
    build: ./services/scheduler
    environment:
      DATABASE_URL: postgresql+asyncpg://app:app@postgres:5432/scheduler_db
      ALEMBIC_DATABASE_URL: postgresql+psycopg2://app:app@postgres:5432/scheduler_db
    depends_on:
      postgres: {condition: service_healthy}
      executor: {condition: service_started}
    ports: ["8080:8080"]

volumes:
  pgdata:
```

```sql
-- scripts/init-dbs.sql  (creates the second database; scheduler_db is created by POSTGRES_DB)
CREATE DATABASE executor_db;
```

```bash
# .env.example
DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/scheduler_db
LOG_LEVEL=INFO
MAX_RETRIES=3
```

- [ ] **Step 2: Commit** — `git add -A && git commit -m "build: docker-compose with postgres + two services"`.

## Task 8.3: Seed script (4 sample tasks)

**Files:** Create `scripts/seed.py`.

- [ ] **Step 1: Implement**

```python
# scripts/seed.py
"""Seed sample tasks. Run: uv run --with httpx python scripts/seed.py"""
import os
from datetime import datetime, timedelta, timezone
import httpx

BASE = os.getenv("SCHEDULER_URL", "http://localhost:8080")


def soon(secs):
    return (datetime.now(timezone.utc) + timedelta(seconds=secs)).isoformat()


TASKS = [
    {"name": "Send Welcome Email", "execution_time": soon(15),
     "webhook_url": "http://executor:8081/send-welcome",
     "payload": {"email": "newuser@example.com", "template": "welcome"},
     "recurrence": "NONE"},
    {"name": "Notify Admin on New Signup", "execution_time": soon(20),
     "webhook_url": "http://executor:8081/notify-admin",
     "payload": {"user": "newuser@example.com"}, "recurrence": "NONE"},
    {"name": "Trigger Daily Summary Report", "execution_time": soon(25),
     "webhook_url": "http://executor:8081/daily-summary",
     "payload": {"report": "daily"}, "recurrence": "DAILY"},
    {"name": "Security Alert Notification", "execution_time": soon(30),
     "webhook_url": "http://executor:8081/security-alert",
     "payload": {"severity": "high"}, "recurrence": "NONE"},
]


def main():
    for t in TASKS:
        r = httpx.post(f"{BASE}/tasks", json=t, timeout=10)
        r.raise_for_status()
        print("created", r.json()["id"], t["name"])


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit** — `git add -A && git commit -m "feat: seed script for 4 sample tasks"`.

## Task 8.4: README

**Files:** Create `README.md` covering: architecture diagram (text), service responsibilities, run via `docker compose up --build`, seed command, Swagger URLs (`http://localhost:8080/docs`, `:8081/docs`), curl examples for each endpoint, lifecycle/status explanation, retry/backoff + polling + recurrence behavior, documented assumptions (single-instance scheduler, create-second-DB via init SQL), how to run tests (`cd services/<svc> && uv run pytest`).

- [ ] **Step 1: Write README.md** (sections above; include the JSON body example from the assignment and a worked async-polling walkthrough).
- [ ] **Step 2: Commit** — `git add -A && git commit -m "docs: project README with setup, architecture, curl examples"`.

## Task 8.5: Full verification

- [ ] **Step 1:** `cd services/scheduler && uv run pytest -q` → all pass.
- [ ] **Step 2:** `cd services/executor && uv run pytest -q` → all pass.
- [ ] **Step 3:** `docker compose up --build -d` (if Docker available), wait for health, then `uv run --with httpx python scripts/seed.py`.
- [ ] **Step 4:** `curl localhost:8080/tasks` → shows seeded tasks; after their execution_time, statuses progress to SUCCESS; daily-summary spawns a child task.
- [ ] **Step 5:** `docker compose down -v`.
- [ ] **Step 6: Commit** any fixups.

---

## Self-Review (completed during planning)

**Spec coverage:**
- POST /tasks + payload shape → Task 1.6/1.7 ✓
- Lifecycle CREATED→PENDING→RUNNING→SUCCESS/FAILED/CANCELLED → transitions (1.1), execution (3.1/4.1), cancel (7.1) ✓
- Retry + exponential backoff + per-attempt logging → 3.1 (attempts), 4.1 (tenacity backoff) ✓
- Async 202 + polling check_url until terminal → 5.1 ✓
- Recurring DAILY/HOURLY/CUSTOM_CRON + auto next run → 6.1/6.2 ✓
- Two services, two DBs, dockerized, compose → 8.1/8.2 ✓
- Structured logs → 1.2 (structlog) used across execution/scheduler ✓
- Swagger → FastAPI auto (`/docs`), noted in README 8.4 ✓
- ≥2 sample tasks preloaded → seed script 8.3 (4 tasks) ✓
- Alembic migrations → 1.4/2.2, run on container start via entrypoint ✓

**Type consistency:** `execute_task(task_id, session_factory)` and `poll_task(task_id, session_factory)` signatures consistent across slices; `register_task_job(task)`/`remove_task_job(task_id)`/`register_poll_job(task)` consistent; `TaskStatus`/`Recurrence`/`AttemptPhase` enum values stable.

**Placeholder scan:** No TBD/TODO; every code step has concrete code. The Slice-1 `scheduler.py` stub is explicitly replaced in Slice 3 (documented).
