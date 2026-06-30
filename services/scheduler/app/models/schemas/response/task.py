from datetime import datetime

from pydantic import BaseModel

from app.utils.enums import Recurrence, TaskStatus


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
