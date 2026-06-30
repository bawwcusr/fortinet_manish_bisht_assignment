from datetime import datetime

from pydantic import BaseModel

from app.utils.enums import ExecutionStatus


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
