from datetime import datetime

from croniter import croniter
from pydantic import BaseModel, Field, model_validator

from app.utils.enums import Recurrence


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
        if self.recurrence == Recurrence.CUSTOM_CRON:
            if not self.cron_expr:
                raise ValueError("cron_expr required when recurrence=CUSTOM_CRON")
            if not croniter.is_valid(self.cron_expr):
                raise ValueError(f"invalid cron expression: {self.cron_expr}")
        return self
