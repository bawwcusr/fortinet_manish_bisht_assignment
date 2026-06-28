# Design: Task Automation & Scheduling System

Date: 2026-06-28
Status: Approved (pending written-spec review)

## 1. Objective

A production-reasonable backend that schedules tasks, triggers webhooks at a
specific time, supports asynchronous webhook processing via polling, handles
retries / failures / recurring executions, and tracks task status accurately.

Two services:

- **Scheduler** — accepts scheduled tasks (REST), persists metadata, fires
  webhooks at the right time, polls async webhooks, manages retries and status
  transitions, auto-schedules recurring runs.
- **Executor** — simulates business logic: receives webhook calls, responds
  synchronously (`2xx`) or asynchronously (`202 Accepted` + polling
  `status_url`), exposes `GET /status/{task_id}`.

## 2. Tech stack (decisions)

| Concern | Choice | Rationale |
| --- | --- | --- |
| Language / framework | Python 3.10+, FastAPI | Spec-preferred, async, auto Swagger |
| Package manager | `uv` | Required by user |
| Scheduling | APScheduler `AsyncIOScheduler` + `SQLAlchemyJobStore` (Postgres) | Native one-off `DateTrigger`; persistent jobs survive restart |
| Retries / backoff | `tenacity` | Existing library, no hand-rolled backoff |
| Cron next-time | `croniter` | `CUSTOM_CRON`; `timedelta` for HOURLY/DAILY |
| HTTP client | `httpx` (async) | Async webhook calls + polling |
| ORM | SQLAlchemy 2.x async (`asyncpg` prod, `aiosqlite` tests) | Modern async, portable types |
| Logging | `structlog` | Structured JSON logs |
| Config | `pydantic-settings` | 12-factor env config |
| DB | PostgreSQL (two DBs: `scheduler_db`, `executor_db`) | Production-grade, matches spec |
| Migrations | Alembic (one migration tree per service) | Versioned schema, production-grade |
| Tests | `pytest`, `pytest-asyncio`, `httpx` ASGITransport | Unit + integration + e2e |
| Containers | Docker + docker-compose | Required by spec |

### Why APScheduler over Celery (documented assumption)

APScheduler natively models "run once at an exact timestamp" (`DateTrigger`) and
recurrence, with a persistent Postgres jobstore so jobs survive restarts. This
fits the assignment with far less infra (no Redis/broker/worker/beat).

**Assumption / tradeoff:** the scheduler runs as a **single instance** (or
leader-elected). For multi-instance HA / very high throughput we would move to a
distributed task queue (Celery + Redis). This is acceptable for the assignment's
scale and is documented in the README.

## 3. Architecture (docker-compose services)

- `postgres` — one container, two databases (`scheduler_db`, `executor_db`).
- `scheduler` — FastAPI app; starts APScheduler in-process on startup.
- `executor` — FastAPI app; own DB; simulates sync + async webhook endpoints.

No Redis. The scheduler process owns the APScheduler instance.

### Layering (both services)

```
api (routers/schemas)  ->  service (business logic)  ->  repository (DB)  ->  models
```

APScheduler job callbacks live in a `scheduling/` module and call into the
service layer — no business logic duplicated in job callbacks.

## 4. Data model

### scheduler_db

`tasks`
- `id` (uuid, pk)
- `name` (str)
- `execution_time` (datetime, UTC)
- `webhook_url` (str)
- `payload` (JSON)
- `recurrence` (enum: NONE | HOURLY | DAILY | CUSTOM_CRON)
- `cron_expr` (str, nullable; required when CUSTOM_CRON)
- `status` (enum: CREATED | PENDING | RUNNING | SUCCESS | FAILED | CANCELLED)
- `max_retries` (int, default from config)
- `retry_count` (int, default 0)
- `check_url` (str, nullable; set for async webhooks)
- `parent_task_id` (uuid, nullable; links a recurring occurrence to its origin)
- `created_at`, `updated_at` (datetime)

`task_attempts` (one row per webhook attempt — satisfies "log each attempt")
- `id` (uuid, pk)
- `task_id` (uuid, fk)
- `attempt_number` (int)
- `phase` (enum: EXECUTE | POLL)
- `started_at`, `finished_at` (datetime)
- `duration_ms` (int)
- `http_status` (int, nullable)
- `response_body` (text, nullable, truncated)
- `error` (text, nullable)

### executor_db

`executions`
- `id` (uuid, pk == task_id supplied by caller, or generated)
- `endpoint` (str)
- `payload` (JSON)
- `status` (enum: QUEUED | RUNNING | SUCCESS | FAILED)
- `created_at`, `updated_at` (datetime)
- `logs` (text)

Portability: use SQLAlchemy generic `JSON` (not `JSONB`) and generic types so
the suite runs on SQLite while prod uses Postgres.

### Migrations (Alembic)

Each service owns an independent Alembic environment (separate `alembic.ini` +
`migrations/` tree, since the two services target separate databases). Schema is
derived from each service's SQLAlchemy `metadata` via autogenerate, then
hand-reviewed. Containers run `alembic upgrade head` on startup (entrypoint)
before serving. Generic column types keep migrations runnable on both Postgres
(prod) and SQLite (tests).

## 5. Lifecycle & data flow

Status: `CREATED -> PENDING -> RUNNING -> SUCCESS | FAILED | CANCELLED`.

1. `POST /tasks` -> validate -> persist `tasks` row (`CREATED` then `PENDING`) ->
   register an APScheduler `DateTrigger` job (id == task id) for `execution_time`.
2. At fire time, the job callback invokes the execution service for the task:
   mark `RUNNING`, record an attempt, then POST `webhook_url` with `payload`.
   - `2xx` (sync) -> `SUCCESS`.
   - `202` (async) -> read `check_url` from body, persist it, schedule a poll job.
   - error / timeout / 5xx -> `tenacity` retry with **exponential backoff** up to
     `max_retries`; on exhaustion -> `FAILED`. Each try logged in `task_attempts`.
3. Poll job: GET `check_url`. `SUCCESS`/`FAILED` are terminal; otherwise re-poll
   with backoff up to a poll cap (config). Each poll logged (phase=POLL).
4. On terminal `SUCCESS`, if `recurrence != NONE`: compute next `execution_time`
   (`croniter` for cron, `timedelta` for HOURLY/DAILY) and create a child task
   row (`parent_task_id` set) scheduled via a new `DateTrigger`.

**Recurrence decision:** one DB row == one run; the next occurrence is created
only after a successful terminal run. This matches the spec wording
("after successful execution, auto-schedule the next run") and keeps per-run
status/attempt history clean. (Alternative considered: APScheduler native
`IntervalTrigger`/`CronTrigger` repeating on one row — rejected because per-run
status tracking gets muddy and a failing run would still re-fire blindly.)

### Cancellation

`POST /tasks/{id}/cancel` -> if not terminal, remove the APScheduler job and set
status `CANCELLED`. Idempotent; rejects cancelling already-terminal tasks.

## 6. API surface

### Scheduler
- `POST /tasks` — create task (body per spec). Returns 201 + task.
- `GET /tasks` — list (filter by status, pagination).
- `GET /tasks/{id}` — task detail incl. status.
- `GET /tasks/{id}/attempts` — attempt/poll history.
- `POST /tasks/{id}/cancel` — cancel.
- `GET /health` — liveness.

### Executor
- `POST /send-welcome` — sync, returns `2xx` + status.
- `POST /notify-admin` — sync.
- `POST /daily-summary` — async, returns `202` + `{status: QUEUED, check_url}`.
- `POST /security-alert` — async, returns `202` + `check_url`.
- `GET /status/{task_id}` — poll status.
- `GET /health` — liveness.

Async endpoints persist a `QUEUED` execution and complete after a simulated
delay (background task) flipping to `SUCCESS` (or `FAILED` for a configurable
fault-injection case to exercise the failure path).

## 7. Error handling & non-functionals

- Structured JSON logs (`structlog`) with `task_id` correlation on every step.
- Status transitions guarded in the service layer; invalid transitions raise.
- Backoff via `tenacity` (`wait_exponential` + cap; jitter optional).
- Timeouts on all `httpx` calls; executor downtime handled via retry path.
- `GET /health` on both services; compose healthchecks gate startup ordering.
- Config via env (`pydantic-settings`): DB URLs, max retries, backoff base/cap,
  poll interval/cap, scan/misfire grace.

## 8. Testing strategy (TDD)

- **Portability:** tests run on SQLite (`aiosqlite`); prod on Postgres. The test
  DB schema is created by running `alembic upgrade head` against the test
  database in a session-scoped fixture (same migrations as prod — keeps schema
  parity). APScheduler is not started in tests — job callbacks
  (execution/poll/recurrence functions) are invoked directly for determinism.
  `httpx` mocked via `respx` or a stub ASGI executor app mounted with
  ASGITransport.
- **Unit:** backoff config, recurrence next-time (cron + interval), status
  transition guard, payload validation.
- **Integration:** API endpoints via `httpx.AsyncClient` + ASGITransport against
  a test DB.
- **End-to-end:** create task -> invoke execution -> stub executor (sync `2xx`,
  async `202` + `/status`) -> assert status reconciles to SUCCESS/FAILED and
  attempts logged; recurrence creates child task.

## 9. Vertical slices (TDD build order)

Each slice: red test -> implement -> green; each leaves a working thread.

1. **Scheduler core**: `POST /tasks` + `GET /tasks/{id}` + persistence + status
   `CREATED/PENDING`. (no firing yet)
2. **Executor sync**: `/send-welcome`, `/notify-admin` + execution persistence +
   `/status`.
3. **Dispatch + sync execution**: job callback calls webhook, logs attempt,
   transitions to SUCCESS/FAILED. Wire APScheduler on startup.
4. **Retries**: `tenacity` exponential backoff, `retry_count`, attempt logs,
   FAILED on exhaustion.
5. **Async + polling**: executor `202` + `/status`; scheduler stores `check_url`,
   poll job reconciles terminal status.
6. **Recurrence**: HOURLY/DAILY/CUSTOM_CRON next-time + child task creation on
   success.
7. **Cancellation**: cancel endpoint + job removal.
8. **Cross-cutting**: Alembic migrations per service, structured logging, health
   checks, Dockerfiles + docker-compose (entrypoint runs `alembic upgrade head`),
   seed script (4 sample tasks), README + Swagger polish.

## 10. Deliverables

- Source for both services, `uv`-managed (`pyproject.toml` + `uv.lock`).
- Alembic migration tree per service (`alembic upgrade head` on container start).
- `Dockerfile` per service + `docker-compose.yml` (postgres + both services).
- `README.md`: setup, run, architecture, assumptions.
- Swagger (FastAPI auto) + curl examples.
- Seed script preloading the 4 sample tasks (Send Welcome Email, Notify Admin on
  New Signup, Trigger Daily Summary Report, Security Alert Notification).
- Test suite (unit + integration + e2e) runnable via `uv run pytest`.

## 11. Out of scope (YAGNI)

- AuthN/Z, multi-tenant, Prometheus metrics, rate limiting (noted as extensions).
- Multi-instance scheduler HA (documented assumption above).
