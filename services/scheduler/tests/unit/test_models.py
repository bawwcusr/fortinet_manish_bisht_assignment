from app.models.task import Task, TaskAttempt
from app.utils.enums import TaskStatus, Recurrence


def test_task_table_columns():
    cols = set(Task.__table__.columns.keys())
    assert {
        "id",
        "name",
        "execution_time",
        "webhook_url",
        "payload",
        "recurrence",
        "cron_expr",
        "status",
        "max_retries",
        "retry_count",
        "check_url",
        "parent_task_id",
        "created_at",
        "updated_at",
    } <= cols


def test_attempt_table_columns():
    cols = set(TaskAttempt.__table__.columns.keys())
    assert {
        "id",
        "task_id",
        "attempt_number",
        "phase",
        "started_at",
        "finished_at",
        "duration_ms",
        "http_status",
        "response_body",
        "error",
    } <= cols
