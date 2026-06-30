from app.exceptions import InvalidCancel, JobRegistrationError, TaskNotFound
from app.models.schemas.request.task import TaskCreate
from app.models.task import Task
from app.repositories.task_repository import TaskRepository
from app.services.scheduler import register_task_job
from app.utils.config import settings
from app.utils.enums import TaskStatus
from app.utils.logging import get_logger
from app.utils.transitions import assert_transition
from app.utils.webhook_validation import assert_webhook_allowed

log = get_logger("service")


class TaskService:
    def __init__(self, session):
        self.repo = TaskRepository(session)
        self.session = session

    async def create(self, dto: TaskCreate) -> Task:
        await assert_webhook_allowed(dto.webhook_url)
        task = Task(
            name=dto.name,
            execution_time=dto.execution_time,
            webhook_url=dto.webhook_url,
            payload=dto.payload,
            recurrence=dto.recurrence,
            cron_expr=dto.cron_expr,
            status=TaskStatus.CREATED,
            max_retries=dto.max_retries
            if dto.max_retries is not None
            else settings.max_retries,
        )
        assert_transition(TaskStatus.CREATED, TaskStatus.PENDING)
        task.status = TaskStatus.PENDING
        task = await self.repo.add(task)
        # Must succeed before request commit; failure rolls back the new task row.
        try:
            register_task_job(task)
        except Exception as exc:
            log.error("job_registration_failed", task_id=task.id, error=str(exc))
            raise JobRegistrationError(task.id) from exc
        return task

    async def get(self, task_id: str) -> Task:
        task = await self.repo.get(task_id)
        if not task:
            raise TaskNotFound(task_id)
        return task

    async def list(self, status=None, limit=100, offset=0):
        return await self.repo.list(status, limit, offset)

    async def cancel(self, task_id: str):
        task = await self.get(task_id)
        if task.status in {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED}:
            raise InvalidCancel(task.status)
        task.status = TaskStatus.CANCELLED
        updated = await self.repo.update(task)
        # DB cancel is authoritative; job removal is best-effort cleanup.
        try:
            from app.services.scheduler import remove_task_job

            remove_task_job(task_id)
        except Exception as exc:
            log.warning("job_removal_failed", task_id=task_id, error=str(exc))
        return updated
