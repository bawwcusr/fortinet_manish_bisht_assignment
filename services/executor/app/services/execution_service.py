import asyncio
from app.exceptions import ExecutionNotFound
from app.models.execution import Execution
from app.repositories.execution_repository import ExecutionRepository
from app.utils.config import settings
from app.utils.enums import ExecutionStatus
from app.utils.logging import get_logger


def _fail_set() -> set[str]:
    # Comma-separated endpoint names that simulate failure (testing / demos).
    return {x.strip() for x in settings.fail_endpoints.split(",") if x.strip()}


class ExecutionService:
    def __init__(self, session):
        self.repo = ExecutionRepository(session)

    async def run_to_completion(self, endpoint: str, payload: dict) -> Execution:
        """Sync webhook mode: simulate business logic, then return the final result in-band."""
        e = Execution(
            endpoint=endpoint, payload=payload, status=ExecutionStatus.RUNNING
        )
        await self.repo.add(e)
        # Stand in for real downstream work (e.g. send email, update CRM).
        await asyncio.sleep(settings.sync_delay_seconds)
        e.status = (
            ExecutionStatus.FAILED
            if endpoint in _fail_set()
            else ExecutionStatus.SUCCESS
        )
        e.logs = f"sync processed endpoint={endpoint}"
        return await self.repo.update(e)

    async def enqueue_async(self, endpoint: str, payload: dict) -> Execution:
        e = Execution(endpoint=endpoint, payload=payload, status=ExecutionStatus.QUEUED)
        return await self.repo.add(e)

    async def get(self, eid: str) -> Execution:
        e = await self.repo.get(eid)
        if not e:
            raise ExecutionNotFound(eid)
        return e


async def complete_after_delay(session_factory, execution_id: str, endpoint: str):
    """Background task: simulate async business logic, then mark terminal.

    Fire-and-forget: any error is logged and swallowed so it never crashes
    the request that scheduled it.
    """
    # Stand in for real downstream work completed after the 202 response.
    await asyncio.sleep(settings.async_delay_seconds)
    try:
        async with session_factory() as s:
            try:
                repo = ExecutionRepository(s)
                e = await repo.get(execution_id)
                if not e:
                    return
                e.status = (
                    ExecutionStatus.FAILED
                    if endpoint in _fail_set()
                    else ExecutionStatus.SUCCESS
                )
                e.logs = (e.logs or "") + f"; async completed status={e.status.value}"
                await repo.update(e)
                await s.commit()
            except Exception as exc:
                await s.rollback()
                raise exc
    except Exception as exc:
        get_logger("executor").warning(
            "async_completion_failed",
            execution_id=execution_id,
            endpoint=endpoint,
            error=str(exc),
        )
