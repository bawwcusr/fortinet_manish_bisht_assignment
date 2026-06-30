"""Registry of simulated webhook endpoints exposed by the executor.

Each entry models a downstream integration; processing delays simulate business
logic rather than network or framework overhead. Add new integrations here only
— no new route handlers required.
"""

WEBHOOKS: dict[str, dict[str, str]] = {
    "send-welcome": {"mode": "sync"},
    "notify-admin": {"mode": "sync"},
    "daily-summary": {"mode": "async"},
    "security-alert": {"mode": "async"},
}


def get_webhook(name: str) -> dict[str, str] | None:
    return WEBHOOKS.get(name)
