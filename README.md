# Task Automation & Scheduling System

A backend for scheduling tasks and firing webhooks at a target time, built as two
FastAPI microservices. The **Scheduler** accepts tasks over REST, validates that
each `webhook_url` targets a registered executor webhook, persists tasks, and
uses APScheduler to fire each webhook at its `execution_time` — with
exponential-backoff retries, support for asynchronous webhooks (HTTP `202` +
polling a `check_url`), recurrence (hourly / daily / custom cron), cancellation,
and a full per-attempt audit trail. The **Executor** is a simulator of downstream
business webhooks: a registry-driven `POST /webhooks/{name}` dispatches to
synchronous handlers (run simulated business logic, then respond after
  `SYNC_DELAY_SECONDS`) or asynchronous handlers (respond
`202 QUEUED`, finish after a delay, and expose `GET /status/{id}` for polling).

## Architecture

```
                +-------------------------------------------------+
   client       |                 Scheduler service               |
  (curl / app)  |  routers -> services -> repositories -> models |
       |        |        |                                         |
       |  POST  |  /tasks (validates webhook vs executor registry)|
       +------> |        v                                         |
                |   APScheduler (AsyncIOScheduler                  |
                |     + SQLAlchemyJobStore)                        |
                |        |  DateTrigger @ execution_time           |
                |        v                                         |
                |   execute job ---- httpx POST webhook_url -----+ |
                |        ^                                       | |
                |        | poll job (GET check_url, async case)  | |
                +--------|---------------------------------------|-+
                         |                                       |
                         |  GET /webhooks (registry)             v
                +--------|-------------------+      +---------------------------+
                |     scheduler_db           |      |     Executor service      |
                |  (tasks, task_attempts)    |      |  routers -> services ->   |
                +----------------------------+      |  repositories -> models   |
                                                    |  webhook_registry (WEBHOOKS)|
                                                    |        |                  |
                                                    |        v                  |
                                                    |   executor_db (executions)|
                                                    +---------------------------+

        PostgreSQL container hosts BOTH databases: scheduler_db + executor_db
        (tests run on SQLite; production runs on Postgres)
```

**Responsibilities**

- **Scheduler** — owns task lifecycle: validates `webhook_url` against the
  executor registry, stores tasks, registers an APScheduler one-off
  `DateTrigger` job per task, fires the webhook at the right time, retries on
  failure, polls async webhooks, advances recurrence, and records every attempt.
- **Executor** — a stand-in for real downstream services (delays simulate
  **business logic** such as sending email or generating a report). A single
  `POST /webhooks/{name}` route dispatches by registry entry: synchronous webhooks
  hold the HTTP connection open for `SYNC_DELAY_SECONDS` while that logic runs,
  then return the terminal result; asynchronous webhooks accept the work
  (`202`), finish the simulated business logic after `ASYNC_DELAY_SECONDS` in a
  background task, and report status through `GET /status/{id}`.

## Tech stack

- **Python 3.10+** with **FastAPI** (async, automatic Swagger UI)
- **uv** for dependency management and running
- **SQLAlchemy 2.x** (async) — `asyncpg` in production, `aiosqlite` in tests
- **Alembic** for migrations (one migration tree per service)
- **APScheduler** (`AsyncIOScheduler` + `SQLAlchemyJobStore`) for time-based jobs
- **tenacity** for retry / exponential backoff
- **croniter** for computing next cron occurrences
- **httpx** (async) for webhook calls and polling
- **structlog** for structured logging
- **Request ID + request logging middleware** on both services (correlation via `X-Request-ID`)
- **pydantic-settings** for 12-factor env configuration
- **pytest** + **pytest-asyncio** + **respx** for testing

## Quick start (Docker)

```bash
# 1. (Optional) create a .env; sensible defaults work out of the box
cp .env.example .env

# 2. Build and start postgres + executor + scheduler
#    (rebuild both app services together — scheduler shares executor's network)
docker compose up --build

# 3. Wait until both services report healthy, then visit:
#    Scheduler API : http://localhost:8080
#    Executor API  : http://localhost:8081
#    Scheduler docs: http://localhost:8080/docs
#    Executor docs : http://localhost:8081/docs
```

Seed four sample tasks (Send Welcome Email, Notify Admin, Daily Summary,
Security Alert). Seeding is **opt-in** (kept out of a normal `up` so restarts
don't create duplicate tasks) via a one-shot `seed` service behind a compose
profile — it waits for the scheduler to be healthy, then POSTs the samples:

```bash
# In-compose seeder (runs inside the network against http://scheduler:8080)
docker compose --profile seed run --rm seed
```

Or seed from the host instead:

```bash
SCHEDULER_URL=http://localhost:8080 uv run --with httpx python scripts/seed.py
```

> Seeded tasks use `http://localhost:8081/webhooks/...` — the same URLs you use
> from curl or Postman. Docker Compose shares the scheduler and executor network
> namespace so the scheduler container can reach the executor at `localhost:8081`.
> Tasks are scheduled to fire a few seconds after seeding — watch the scheduler
> logs and then `GET /tasks` to see them transition.

Tear everything down (including the Postgres volume):

```bash
docker compose down -v
```

## Postman

A ready-made collection and local environment live in `postman/`:

| File | Purpose |
| --- | --- |
| `postman/Task-Automation-Scheduling.postman_collection.json` | All scheduler + executor endpoints, examples, and an end-to-end flow |
| `postman/local.postman_environment.json` | `scheduler_base_url`, `executor_base_url`, and auto-populated `task_id` / `execution_id` |

**Import in Postman**

1. **Import** → drag both JSON files (or **File** → **Import**).
2. Select the **Task Automation — Local** environment in the top-right dropdown.
3. With services running (`docker compose up`), run requests from the collection.

The collection includes request descriptions, query parameters, example bodies for
all four registered webhooks, error-case examples (`422` invalid webhook, `404`
unknown executor webhook), and test scripts that save `task_id` and `execution_id`
from responses. Use `http://localhost:8080` and `http://localhost:8081` everywhere
(Docker Compose, local dev, Postman, curl, and seed script).

Swagger UI remains available at `/docs` on each service for interactive exploration.

## Local development (without Docker)

Run each service independently. They default to local SQLite databases, so no
Postgres is required for local hacking.

Scheduler:

```bash
cd services/scheduler
uv sync
# Optional: point at Postgres instead of the SQLite defaults
# export DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/scheduler_db
# export ALEMBIC_DATABASE_URL=postgresql+psycopg2://app:app@localhost:5432/scheduler_db
# When set, POST /tasks rejects webhook_url not in the executor registry:
export EXECUTOR_BASE_URL=http://localhost:8081
uv run alembic upgrade head
uv run uvicorn app.main:app --port 8080
```

Executor:

```bash
cd services/executor
uv sync
# Optional Postgres overrides:
# export DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/executor_db
# export ALEMBIC_DATABASE_URL=postgresql+psycopg2://app:app@localhost:5432/executor_db
uv run alembic upgrade head
uv run uvicorn app.main:app --port 8081
```

If you leave `DATABASE_URL` / `ALEMBIC_DATABASE_URL` unset, the scheduler uses
`sqlite+aiosqlite:///./scheduler.db` and the executor uses
`sqlite+aiosqlite:///./executor.db`.

## API reference (Scheduler)

Base URL: `http://localhost:8080`

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/tasks` | Create a task. Returns `201` with the task (status `PENDING`). `422` if `webhook_url` is not a registered executor webhook; `503` if APScheduler job registration fails (DB row is rolled back). |
| `GET` | `/tasks` | List tasks. Optional `status` filter, plus `limit` / `offset`. |
| `GET` | `/tasks/{id}` | Fetch one task. `404` if it does not exist. |
| `DELETE` | `/tasks/{id}` | Cancel a non-terminal task (`CANCELLED`). `200` → final state; `404` missing; `409` if already terminal. |
| `GET` | `/health` | Liveness check (`{"status": "ok"}`). |

Per-attempt audit rows (`task_attempts`) are written internally during execution
and polling; they are not exposed via a public API endpoint.

### Create task request body

`recurrence` must be one of `NONE`, `HOURLY`, `DAILY`, `CUSTOM_CRON`. `cron_expr`
is required only when `recurrence` is `CUSTOM_CRON`. `max_retries` is optional
(falls back to the service default). `execution_time` is an ISO-8601 timestamp.

When `EXECUTOR_BASE_URL` is set, `webhook_url` must be
`{EXECUTOR_BASE_URL}/webhooks/{name}` where `{name}` exists in the executor's
`GET /webhooks` registry. Omit `EXECUTOR_BASE_URL` to skip validation (e.g. in
unit tests).

```json
{
  "name": "Send Welcome Email",
  "execution_time": "2026-06-28T12:00:00Z",
  "webhook_url": "http://localhost:8081/webhooks/send-welcome",
  "payload": { "email": "newuser@example.com", "template": "welcome" },
  "recurrence": "NONE",
  "cron_expr": null,
  "max_retries": 3
}
```

### curl examples

Create:

```bash
curl -X POST http://localhost:8080/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Send Welcome Email",
    "execution_time": "2026-06-28T12:00:00Z",
    "webhook_url": "http://localhost:8081/webhooks/send-welcome",
    "payload": {"email": "newuser@example.com", "template": "welcome"},
    "recurrence": "NONE"
  }'
```

Get one task:

```bash
curl http://localhost:8080/tasks/<task_id>
```

List (optionally filtered / paginated):

```bash
curl "http://localhost:8080/tasks?status=PENDING&limit=50&offset=0"
```

Cancel:

```bash
curl -X DELETE http://localhost:8080/tasks/<task_id>
```

## API reference (Executor)

Base URL: `http://localhost:8081`

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/webhooks` | List registered webhook names and modes (source of truth for scheduler validation). |
| `POST` | `/webhooks/{name}` | Invoke a registered webhook. Sync names return `200`; async names return `202` + `check_url`. `404` if unknown. |
| `GET` | `/status/{id}` | Fetch an execution's current status. `404` if unknown. |
| `GET` | `/health` | Liveness check (`{"status": "ok"}`). |

Registered webhooks (`app/utils/webhook_registry.py`):

| Name | Mode |
| --- | --- |
| `send-welcome` | sync |
| `notify-admin` | sync |
| `daily-summary` | async |
| `security-alert` | async |

Add new simulated integrations by extending `WEBHOOKS` in the registry — no new
routes required. After adding a name, restart the executor (or wait for the
scheduler's 60s registry cache to expire) before scheduling tasks that target it.

List registered webhooks:

```bash
curl http://localhost:8081/webhooks
```

All webhook invocations accept an arbitrary JSON `payload` object as the request body.

Synchronous example:

```bash
curl -X POST http://localhost:8081/webhooks/send-welcome \
  -H 'Content-Type: application/json' \
  -d '{"email": "newuser@example.com"}'
```

Asynchronous example (note the returned `check_url`):

```bash
curl -X POST http://localhost:8081/webhooks/daily-summary \
  -H 'Content-Type: application/json' \
  -d '{"report": "daily"}'
# -> 202 {"status": "QUEUED", "check_url": "http://localhost:8081/status/<id>"}

curl http://localhost:8081/status/<id>
```

**Config knobs** (see also [Configuration reference](#configuration-reference)):

- `SYNC_DELAY_SECONDS` (default `5.0`) — simulated **business logic** runtime
  before a sync webhook returns its terminal `200`/`500` response (scheduler task
  stays `RUNNING` for this duration).
- `ASYNC_DELAY_SECONDS` (default `5.0`) — simulated **business logic** runtime
  before an async execution flips to its terminal status.
- `FAIL_ENDPOINTS` (default empty) — comma-separated list of webhook names that
  should resolve to `FAILED` instead of `SUCCESS` (fault injection for testing
  the failure path), e.g. `FAIL_ENDPOINTS=security-alert,daily-summary`.

## Task lifecycle & status

```
CREATED -> PENDING -> RUNNING -> SUCCESS
                              \-> FAILED
                  (any non-terminal) -> CANCELLED
```

- **CREATED** — the row is being constructed at creation time.
- **PENDING** — persisted and scheduled; an APScheduler job is registered for
  `execution_time` and the task is waiting to fire.
- **RUNNING** — the job has fired; the webhook is being called (and, for async
  webhooks, the task stays `RUNNING` while it is polled).
- **SUCCESS** — a synchronous `2xx` (non-`202`) response, or an async poll that
  observed `SUCCESS`. Terminal.
- **FAILED** — retries exhausted, a non-retryable bad response, or the poll cap
  was reached without a terminal result. Terminal.
- **CANCELLED** — explicitly cancelled while non-terminal; the APScheduler job is
  removed. Terminal.

Terminal states (`SUCCESS`, `FAILED`, `CANCELLED`) are never re-entered; an
execution that finds the task already terminal short-circuits.

## Webhook validation (scheduler ↔ executor)

When `EXECUTOR_BASE_URL` is configured (set automatically in `docker-compose.yml`),
`POST /tasks` validates the task before persisting or scheduling:

1. `webhook_url` must start with `{EXECUTOR_BASE_URL}/webhooks/`.
2. The scheduler fetches `GET {EXECUTOR_BASE_URL}/webhooks` (cached 60 seconds).
3. The path segment after `/webhooks/` must match a key in the returned registry.

Failures return **422** (`InvalidWebhook`) — e.g. wrong URL shape, unknown webhook
name, or executor registry unreachable. This ensures only executable webhooks are
scheduled. Recurring child tasks inherit the parent's already-validated
`webhook_url`.

Relevant config:

- `EXECUTOR_BASE_URL` — base URL of the executor (`http://localhost:8081` in
  compose and local dev). Unset disables validation.
- `WEBHOOK_VALIDATION_TIMEOUT` (default `2.0`) — timeout for the registry fetch.

## Retry & backoff

Webhook delivery is wrapped in `tenacity` with **exponential backoff**. A retry
is triggered on:

- network / connection errors and timeouts, and
- responses with HTTP `5xx`, `408`, or `429`.

The total number of webhook attempts is capped by the task's `max_retries`
(implemented via tenacity's `stop_after_attempt`). **`max_retries` is the TOTAL
attempt cap**, not the number of additional retries beyond the first — e.g.
`max_retries=3` means at most three webhook attempts. When the cap is exhausted
the task becomes `FAILED`.

Every individual attempt (success or failure) is recorded as a `TaskAttempt` row
(phase `EXECUTE`) with timing, HTTP status, a truncated response body, and any
error — stored in `task_attempts` (internal audit; not exposed via API).

Relevant config:

- `MAX_RETRIES` (default `3`) — total attempt cap used when a task omits `max_retries`.
- `BACKOFF_BASE_SECONDS` (default `1.0`) — exponential backoff multiplier.
- `BACKOFF_MAX_SECONDS` (default `60.0`) — backoff ceiling.
- `HTTP_TIMEOUT_SECONDS` (default `10.0`) — per-request timeout.

## Asynchronous execution (202 + polling)

When a webhook returns **`202 Accepted`**, the scheduler:

1. reads `check_url` from the response body and persists it on the task,
2. keeps the task `RUNNING` (it is not yet done), and
3. registers a separate poll job.

The poll job issues `GET check_url` repeatedly with backoff, reading the
`status` field from each response. `SUCCESS` and `FAILED` are terminal; anything
else means "still working", so it polls again until a terminal status arrives or
the poll cap is hit (which resolves the task to `FAILED`). Each poll is recorded
as a `TaskAttempt` row with phase `POLL`.

Worked walkthrough (daily-summary):

1. Task fires; scheduler `POST`s `http://localhost:8081/webhooks/daily-summary`.
2. Executor stores a `QUEUED` execution and returns
   `202 {"status": "QUEUED", "check_url": ".../status/<id>"}`.
3. Scheduler stores `check_url`, leaves the task `RUNNING`, schedules a poll job.
4. After `ASYNC_DELAY_SECONDS` of simulated business logic, the executor flips
   the execution to `SUCCESS`.
5. The next poll `GET`s `check_url`, sees `"status": "SUCCESS"`, and marks the
   task `SUCCESS` (then advances recurrence if applicable).

Relevant config:

- `POLL_MAX_ATTEMPTS` (default `10`) — maximum number of poll attempts before the
  task is failed.
- `POLL_INTERVAL_SECONDS` (default `5.0`) — base interval / backoff multiplier
  between polls (also the initial delay before the first poll).

## Recurring tasks

When a task with `recurrence != NONE` reaches a terminal **`SUCCESS`**, the
scheduler computes the next occurrence and creates a **child task**:

- **HOURLY** — next run = previous `execution_time` + 1 hour.
- **DAILY** — next run = previous `execution_time` + 1 day.
- **CUSTOM_CRON** — next run computed from `cron_expr` via `croniter`.

The child is a new `tasks` row (status `PENDING`) with `parent_task_id` pointing
at the task it descends from, and a fresh APScheduler `DateTrigger` job is
registered for it. Recurrence is **advanced only on success** — a `FAILED` or
`CANCELLED` run does not schedule a follow-up. This keeps one DB row per run, so
status and attempt history stay clean per occurrence.

## Configuration reference

See `.env.example` for a full template. Scheduler settings (read via
`pydantic-settings`):

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | `sqlite+aiosqlite:///./scheduler.db` | Async SQLAlchemy URL |
| `ALEMBIC_DATABASE_URL` | derived from `DATABASE_URL` | Sync URL for Alembic + APScheduler job store |
| `LOG_LEVEL` | `INFO` | structlog level |
| `MAX_RETRIES` | `3` | Default total webhook attempt cap |
| `BACKOFF_BASE_SECONDS` | `1.0` | Retry backoff base |
| `BACKOFF_MAX_SECONDS` | `60.0` | Retry backoff ceiling |
| `HTTP_TIMEOUT_SECONDS` | `10.0` | Per-request webhook/poll timeout |
| `POLL_MAX_ATTEMPTS` | `10` | Max poll rounds before `FAILED` |
| `POLL_INTERVAL_SECONDS` | `5.0` | Poll delay / backoff base |
| `SCHEDULER_MISFIRE_GRACE_SECONDS` | `30` | APScheduler misfire grace window |
| `EXECUTOR_BASE_URL` | unset | Enables webhook registry validation on create |
| `WEBHOOK_VALIDATION_TIMEOUT` | `2.0` | Timeout for `GET /webhooks` during validation |
| `DB_POOL_SIZE` | `20` | SQLAlchemy pool size (Postgres only) |
| `DB_MAX_OVERFLOW` | `10` | Extra connections beyond pool size |
| `DB_POOL_PRE_PING` | `true` | Drop stale connections before checkout |

Executor settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | `sqlite+aiosqlite:///./executor.db` | Async SQLAlchemy URL |
| `ALEMBIC_DATABASE_URL` | derived from `DATABASE_URL` | Sync URL for Alembic |
| `SYNC_DELAY_SECONDS` | `5.0` | Simulated business-logic runtime for sync webhooks (holds HTTP open) |
| `ASYNC_DELAY_SECONDS` | `5.0` | Simulated business-logic runtime for async webhooks |
| `ASYNC_MAX_IN_FLIGHT` | `32` | Max concurrent async completion tasks |
| `FAIL_ENDPOINTS` | empty | Comma-separated webhook names that resolve to `FAILED` |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` / `DB_POOL_PRE_PING` | `20` / `10` / `true` | Postgres pool tuning (same as scheduler) |

## Database & migrations

- Two logical databases on one Postgres container: `scheduler_db` (tasks +
  attempts) and `executor_db` (executions).
- `scheduler_db` is created by Postgres via `POSTGRES_DB`; `executor_db` is
  created by `scripts/init-dbs.sql`, which Postgres runs on first init.
- Each service has its own independent Alembic environment (`alembic.ini` +
  `migrations/`), since they target separate databases.
- Migrations run automatically on container start: each service's
  `entrypoint.sh` runs `alembic upgrade head` before launching uvicorn.
- Each service reads `DATABASE_URL` (async driver) and `ALEMBIC_DATABASE_URL`
  (sync driver, used by Alembic).

## Running tests

```bash
cd services/scheduler && uv run pytest
cd services/executor && uv run pytest
```

Both suites use SQLite in-memory databases, `respx` for HTTP mocking, and do not
require Postgres or Docker.

## Project layout

```
.
├── docker-compose.yml          # postgres + scheduler + executor
├── .env.example                # env template (defaults match compose)
├── postman/
│   ├── Task-Automation-Scheduling.postman_collection.json
│   └── local.postman_environment.json
├── scripts/
│   ├── init-dbs.sql            # creates executor_db
│   └── seed.py                 # seeds 4 sample tasks
└── services/
    ├── scheduler/
    │   ├── Dockerfile
    │   ├── entrypoint.sh       # alembic upgrade head -> uvicorn :8080
    │   ├── alembic.ini
    │   ├── migrations/
    │   ├── pyproject.toml
    │   ├── tests/
    │   └── app/
    │       ├── main.py                 # FastAPI app factory + lifespan
    │       ├── db.py                   # engine / session / Base
    │       ├── exceptions.py           # domain errors (TaskNotFound, InvalidWebhook, …)
    │       ├── exception_handlers.py   # AppException → JSONResponse
    │       ├── middleware/             # request ID + request logging
    │       ├── repositories/
    │       │   └── task_repository.py
    │       ├── routers/
    │       │   └── tasks.py            # POST/GET/DELETE /tasks
    │       ├── models/
    │       │   ├── task.py             # Task, TaskAttempt (SQLAlchemy)
    │       │   └── schemas/
    │       │       ├── request/        # TaskCreate
    │       │       └── response/       # TaskRead
    │       ├── services/
    │       │   ├── task_service.py     # create / get / list / cancel
    │       │   ├── execution.py        # webhook execute + poll + recurrence
    │       │   └── scheduler.py        # APScheduler jobs
    │       └── utils/
    │           ├── deps.py             # get_db (commit/rollback per request)
    │           ├── config.py           # pydantic-settings
    │           ├── logging.py          # structlog setup
    │           ├── enums.py            # TaskStatus, Recurrence, AttemptPhase
    │           ├── transitions.py      # status transition guards
    │           ├── recurrence.py       # next-run computation
    │           ├── webhook_validation.py  # executor registry check on create
    │           └── types.py            # TZDateTime
    └── executor/
        ├── Dockerfile
        ├── entrypoint.sh       # alembic upgrade head -> uvicorn :8081
        ├── alembic.ini
        ├── migrations/
        ├── pyproject.toml
        ├── tests/
        └── app/
            ├── main.py
            ├── db.py
            ├── exceptions.py           # ExecutionNotFound, WebhookNotFound
            ├── exception_handlers.py
            ├── middleware/             # request ID + request logging
            ├── repositories/
            │   └── execution_repository.py
            ├── routers/
            │   └── webhooks.py       # GET /webhooks, POST /webhooks/{name}, GET /status/{id}
            ├── models/
            │   ├── execution.py      # Execution (SQLAlchemy)
            │   └── schemas/
            │       └── response/     # ExecutionRead, AsyncAccepted
            ├── services/
            │   └── execution_service.py
            └── utils/
                ├── deps.py
                ├── config.py
                ├── logging.py
                ├── enums.py
                ├── types.py
                └── webhook_registry.py  # WEBHOOKS dict (single source of truth)
```
