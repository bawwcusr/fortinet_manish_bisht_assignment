"""Executor webhook simulator: registry-driven sync and async endpoints.

Delays (SYNC_DELAY_SECONDS / ASYNC_DELAY_SECONDS) stand in for real downstream
business logic — e.g. sending email, calling APIs, generating reports.
"""

from fastapi import (
    APIRouter,
    Depends,
    Request,
    Response,
)
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import SessionLocal
from app.exceptions import WebhookNotFound
from app.models.schemas import AsyncAccepted, ExecutionRead
from app.services.execution_service import ExecutionService
from app.utils.deps import get_db
from app.utils.enums import ExecutionStatus
from app.utils.async_workers import spawn_async_completion
from app.utils.webhook_registry import get_webhook

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/webhooks")
async def list_webhooks():
    """Return registered webhook names (source of truth for scheduler validation)."""
    from app.utils.webhook_registry import WEBHOOKS

    return {"webhooks": WEBHOOKS}


async def _run_to_completion(endpoint: str, payload: dict, db, response: Response):
    e = await ExecutionService(db).run_to_completion(endpoint, payload)
    # Scheduler treats non-2xx as failure; sync failures return 500 not 200+FAILED.
    if e.status == ExecutionStatus.FAILED:
        response.status_code = 500
    return ExecutionRead.model_validate(e)


async def _async(endpoint, payload, request, db, response: Response):
    e = await ExecutionService(db).enqueue_async(endpoint, payload)
    spawn_async_completion(SessionLocal, e.id, endpoint)
    base = str(request.base_url).rstrip("/")
    response.status_code = 202
    return AsyncAccepted(check_url=f"{base}/status/{e.id}")


@router.post("/webhooks/{name}")
async def invoke_webhook(
    name: str,
    payload: dict,
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """Dispatch to sync or async handling based on webhook_registry.WEBHOOKS."""
    spec = get_webhook(name)
    if spec is None:
        raise WebhookNotFound(name)
    if spec["mode"] == "sync":
        return await _run_to_completion(name, payload, db, response)
    if spec["mode"] == "async":
        return await _async(name, payload, request, db, response)
    raise WebhookNotFound(name)


@router.get("/status/{execution_id}", response_model=ExecutionRead)
async def status(execution_id: str, db: AsyncSession = Depends(get_db)):
    return await ExecutionService(db).get(execution_id)
