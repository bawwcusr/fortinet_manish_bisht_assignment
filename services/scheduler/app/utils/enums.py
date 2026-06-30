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
