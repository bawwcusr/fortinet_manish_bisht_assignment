"""Seed sample tasks.

Host:   SCHEDULER_URL=http://localhost:8080 uv run --with httpx python scripts/seed.py
Docker: docker compose --profile seed run --rm seed
"""

import os
import time
from datetime import datetime, timedelta, timezone
import httpx

BASE = os.getenv("SCHEDULER_URL", "http://localhost:8080")


def soon(secs):
    return (datetime.now(timezone.utc) + timedelta(seconds=secs)).isoformat()


TASKS = [
    {
        "name": "Send Welcome Email",
        "execution_time": soon(15),
        "webhook_url": "http://localhost:8081/webhooks/send-welcome",
        "payload": {"email": "newuser@example.com", "template": "welcome"},
        "recurrence": "NONE",
    },
    {
        "name": "Notify Admin on New Signup",
        "execution_time": soon(20),
        "webhook_url": "http://localhost:8081/webhooks/notify-admin",
        "payload": {"user": "newuser@example.com"},
        "recurrence": "NONE",
    },
    {
        "name": "Trigger Daily Summary Report",
        "execution_time": soon(25),
        "webhook_url": "http://localhost:8081/webhooks/daily-summary",
        "payload": {"report": "daily"},
        "recurrence": "DAILY",
    },
    {
        "name": "Security Alert Notification",
        "execution_time": soon(30),
        "webhook_url": "http://localhost:8081/webhooks/security-alert",
        "payload": {"severity": "high"},
        "recurrence": "NONE",
    },
]


def wait_for_health(timeout=60.0):
    """Block until the scheduler answers /health (useful inside compose)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{BASE}/health", timeout=2).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(1)
    raise RuntimeError(f"scheduler not healthy at {BASE} after {timeout}s")


def main():
    wait_for_health()
    for t in TASKS:
        r = httpx.post(f"{BASE}/tasks", json=t, timeout=10)
        r.raise_for_status()
        print("created", r.json()["id"], t["name"])


if __name__ == "__main__":
    main()
