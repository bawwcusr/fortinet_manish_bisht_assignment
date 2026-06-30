"""Validate task webhook_url against the executor registry before scheduling."""

import time

import httpx

from app.exceptions import InvalidWebhook
from app.utils.config import settings

_CACHE: dict = {"webhooks": None, "expires": 0.0}
_CACHE_TTL_SECONDS = 60.0


def _webhook_prefix() -> str | None:
    if not settings.executor_base_url:
        return None
    return f"{settings.executor_base_url.rstrip('/')}/webhooks/"


def parse_webhook_name(webhook_url: str) -> str | None:
    """Extract registry name from webhook_url, or None if URL shape is wrong."""
    prefix = _webhook_prefix()
    if prefix is None:
        return None
    if not webhook_url.startswith(prefix):
        return None
    name = webhook_url[len(prefix) :].split("?")[0].strip("/")
    return name or None


async def _fetch_registry() -> dict[str, dict]:
    now = time.monotonic()
    if _CACHE["webhooks"] is not None and now < _CACHE["expires"]:
        return _CACHE["webhooks"]

    url = f"{settings.executor_base_url.rstrip('/')}/webhooks"
    async with httpx.AsyncClient(timeout=settings.webhook_validation_timeout) as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()

    webhooks = data.get("webhooks", {})
    _CACHE["webhooks"] = webhooks
    _CACHE["expires"] = now + _CACHE_TTL_SECONDS
    return webhooks


async def assert_webhook_allowed(webhook_url: str) -> None:
    """Reject tasks whose webhook is not registered on the executor.

    Skipped when executor_base_url is unset (e.g. unit tests). When set, the URL
    must be {executor_base_url}/webhooks/{name} and name must exist in the registry.
    """
    prefix = _webhook_prefix()
    if prefix is None:
        return

    name = parse_webhook_name(webhook_url)
    if name is None:
        raise InvalidWebhook(
            f"webhook_url must be under {prefix}{{name}} (executor registry)"
        )

    try:
        registry = await _fetch_registry()
    except Exception as exc:
        raise InvalidWebhook("executor registry unavailable") from exc

    if name not in registry:
        raise InvalidWebhook(f"unknown webhook: {name}")
