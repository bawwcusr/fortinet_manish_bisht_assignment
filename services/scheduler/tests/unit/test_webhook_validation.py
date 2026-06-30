import respx
from httpx import Response

from app.exceptions import InvalidWebhook
from app.utils import webhook_validation
from app.utils.webhook_validation import assert_webhook_allowed, parse_webhook_name


def _reset_cache() -> None:
    webhook_validation._CACHE["webhooks"] = None
    webhook_validation._CACHE["expires"] = 0.0


def test_parse_webhook_name(monkeypatch):
    monkeypatch.setattr(
        "app.utils.webhook_validation.settings.executor_base_url",
        "http://localhost:8081",
    )
    assert (
        parse_webhook_name("http://localhost:8081/webhooks/send-welcome")
        == "send-welcome"
    )
    assert parse_webhook_name("http://other/webhooks/send-welcome") is None


@respx.mock
async def test_assert_webhook_allowed_rejects_unknown(monkeypatch):
    _reset_cache()
    monkeypatch.setattr(
        "app.utils.webhook_validation.settings.executor_base_url",
        "http://localhost:8081",
    )
    respx.get("http://localhost:8081/webhooks").mock(
        return_value=Response(
            200, json={"webhooks": {"send-welcome": {"mode": "sync"}}}
        )
    )
    try:
        await assert_webhook_allowed("http://localhost:8081/webhooks/unknown")
        assert False, "expected InvalidWebhook"
    except InvalidWebhook as exc:
        assert "unknown webhook" in exc.message


@respx.mock
async def test_assert_webhook_allowed_accepts_registered(monkeypatch):
    _reset_cache()
    monkeypatch.setattr(
        "app.utils.webhook_validation.settings.executor_base_url",
        "http://localhost:8081",
    )
    respx.get("http://localhost:8081/webhooks").mock(
        return_value=Response(
            200, json={"webhooks": {"send-welcome": {"mode": "sync"}}}
        )
    )
    await assert_webhook_allowed("http://localhost:8081/webhooks/send-welcome")


async def test_assert_webhook_skipped_when_executor_base_url_unset(monkeypatch):
    monkeypatch.setattr("app.utils.webhook_validation.settings.executor_base_url", None)
    await assert_webhook_allowed("http://anything/invalid")
