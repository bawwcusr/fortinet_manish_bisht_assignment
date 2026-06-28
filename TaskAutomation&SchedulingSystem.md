# Assignment: Task Automation & Scheduling System

## Objective

Design and implement a **production-ready backend system** that can:

- Schedule tasks to be executed at a specific time
- Trigger **webhooks** at the right time
- Support **asynchronous webhook processing** (via polling)
- Handle **task retries**, **failures**, and **recurring executions**
- Track task **status** accurately
- Be easily extensible for future use cases

This system should be designed as a **microservice-oriented backend application** using modern best practices, proper modularity, and clean code.

---

## What You Will Build

The solution should be split into the following two services:

### Task Scheduler Service

- Responsible for:
  - Accepting new scheduled tasks (via REST API)
  - Persisting metadata in the **scheduler database**
  - Triggering webhook execution at the appropriate time
  - Polling webhook status if the response is asynchronous
  - Managing retries and status transitions
- This service should be **generic**, i.e., it should support executing **any webhook** (extensible, not hardcoded).

### Task Executor (Webhook Processor)

- Simulates a business logic service that:
  - Receives webhook calls
  - Processes tasks either **synchronously** (returns `2xx` with status) or
    **asynchronously** (returns `202 Accepted` with a polling `status_url`)
  - Updates execution status internally
  - Exposes `/status/{task_id}` endpoint for polling

---

## System Architecture

- Two **separate services** (`scheduler`, `executor`)
- Two **separate databases**:
  - `Scheduler DB` → stores task metadata, schedule, status, retries
  - `Executor DB` → stores actual task execution info, payloads, logs
- Services should be dockerized and runnable via `docker-compose`
- All APIs should be testable using preferably Swagger documentation (**Postman** or **cURL**)

---

## Functional Requirements

### Task Creation API (`POST /tasks`)

- Accept a JSON payload like:

```json
{
  "name": "Send Welcome Email",
  "execution_time": "2025-09-02T10:30:00Z",
  "webhook_url": "http://executor:8081/send-welcome",
  "payload": {
    "email": "newuser@example.com",
    "template": "welcome"
  },
  "recurrence": "NONE" // Optional: DAILY, HOURLY, etc.
}
```

### Task Lifecycle

```text
CREATED → PENDING → RUNNING → SUCCESS / FAILED / CANCELLED
```

### Failure & Retry

- Tasks should retry on failure (configurable max retries)
- Use **exponential backoff** for retries
- Log each attempt with timestamp, duration, HTTP status, and response

### Asynchronous Execution

- Some webhook responses may return `202 Accepted` with:

```json
{
  "status": "QUEUED",
  "check_url": "http://executor:8081/status/abc123"
}
```

- Scheduler must **poll this endpoint** until task is marked `SUCCESS` or `FAILED`

### Recurring Tasks

- Support `DAILY`, `HOURLY`, or `CUSTOM_CRON` recurrence types
- After successful execution, auto-schedule the next run

---

## Non-Functional Expectations

- Production-quality logging (structured logs)
- Clear error handling and status tracking
- Clean and modular code structure (e.g., controller → service → db layer)
- Containerized setup with:

  - `docker-compose.yml`
  - `Dockerfile` for each service

- API Documentation or Postman Collection

---

## Sample Use Cases

You should pre-load at least **2 sample tasks** into the system:

1. **Send Welcome Email**
2. **Notify Admin on New Signup**
3. **Trigger Daily Summary Report**
4. **Security Alert Notification**

You may simulate these in the **Executor** service with stubbed endpoints and mock delays or async behavior.

---

## Deliverables

- Full source code for both services
- `README.md` with setup, run instructions, and architecture explanation
- Docker setup (Dockerfile + docker-compose)
- Swagger documentation
- At least 2 sample tasks (pre-loaded or script)

---

## Evaluation Criteria

| Area                    | What We're Looking For                                   |
| ----------------------- | -------------------------------------------------------- |
| Architecture Design     | Clear separation of concerns, extensibility              |
| Coding Practices        | Modular code, logging, error handling, retries           |
| Task Scheduling Logic   | Accuracy, reliability, clean status transitions          |
| Async Handling          | Polling flow, edge cases, long-running task handling     |
| Documentation & Clarity | Can we run and understand your system with minimal help? |
| Dockerization           | Services should run seamlessly in containers             |

---

## Tech Guidelines

- **Language:** Python (FastAPI preferred), Java (Spring Boot), or Node.js
- **Database:** Postgres / MySQL / SQLite (use what you're comfortable with)
- **Job Scheduling:** You may implement your own OR use a library (like APScheduler / Celery / Quartz)
- Please focus on **completeness**, **correctness**, and **code quality**, rather than building an overly complex system.

---

## Questions?

If anything is unclear, please document your assumptions directly in the README or code comments. We'd rather see your problem-solving thought process than endless email threads.

---

**Best of luck!**
We’re excited to see how you design and build under real-world constraints.
