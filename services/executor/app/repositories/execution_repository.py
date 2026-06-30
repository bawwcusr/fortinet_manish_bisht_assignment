from sqlalchemy.ext.asyncio import AsyncSession

from app.models.execution import Execution


class ExecutionRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(self, e: Execution) -> Execution:
        self.session.add(e)
        await self.session.flush()
        return e

    async def get(self, eid: str) -> Execution | None:
        return await self.session.get(Execution, eid)

    async def update(self, e: Execution) -> Execution:
        await self.session.flush()
        return e
