"""Shared outbound HTTP client for webhook dispatch and polling."""

import httpx

from app.utils.config import settings

_client: httpx.AsyncClient | None = None


def get_http_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=settings.http_timeout_seconds,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )
    return _client
