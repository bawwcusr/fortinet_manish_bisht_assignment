from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schemas import TaskCreate, TaskRead
from app.services.task_service import TaskService
from app.utils.deps import get_db
from app.utils.enums import TaskStatus

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/tasks", response_model=TaskRead, status_code=201)
async def create_task(dto: TaskCreate, db: AsyncSession = Depends(get_db)):
    return await TaskService(db).create(dto)


@router.get("/tasks", response_model=list[TaskRead])
async def list_tasks(
    status: TaskStatus | None = None,
    limit: int = Query(100, le=500),
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    return await TaskService(db).list(status, limit, offset)


@router.get("/tasks/{task_id}", response_model=TaskRead)
async def get_task(task_id: str, db: AsyncSession = Depends(get_db)):
    return await TaskService(db).get(task_id)


@router.delete("/tasks/{task_id}", response_model=TaskRead)
async def delete_task(task_id: str, db: AsyncSession = Depends(get_db)):
    return await TaskService(db).cancel(task_id)
