from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.task import Task, TaskAttempt
from app.utils.enums import TaskStatus


class TaskRepository:

    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(self, task: Task) -> Task:
        self.session.add(task)
        await self.session.flush()
        return task

    async def get(self, task_id: str) -> Task | None:
        return await self.session.get(Task, task_id)

    async def list(
        self, status: TaskStatus | None = None, limit: int = 100, offset: int = 0
    ):
        stmt = select(Task).order_by(Task.created_at.desc()).limit(limit).offset(offset)
        if status:
            stmt = stmt.where(Task.status == status)
        return list((await self.session.scalars(stmt)).all())

    async def list_due(self, now):
        stmt = select(Task).where(
            Task.status == TaskStatus.PENDING, Task.execution_time <= now
        )
        return list((await self.session.scalars(stmt)).all())

    async def update(self, task: Task) -> Task:
        await self.session.flush()
        return task

    async def add_attempt(self, attempt: TaskAttempt) -> TaskAttempt:
        self.session.add(attempt)
        await self.session.flush()
        return attempt

    async def attempts_for(self, task_id: str):
        stmt = (
            select(TaskAttempt)
            .where(TaskAttempt.task_id == task_id)
            .order_by(TaskAttempt.started_at.asc())
        )
        return list((await self.session.scalars(stmt)).all())
