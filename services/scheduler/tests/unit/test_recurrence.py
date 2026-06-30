from datetime import datetime, timezone, timedelta

from app.utils.enums import Recurrence
from app.utils.recurrence import next_run_time


def test_hourly_adds_one_hour():
    base = datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert next_run_time(Recurrence.HOURLY, base, None) == base + timedelta(hours=1)


def test_daily_adds_one_day():
    base = datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert next_run_time(Recurrence.DAILY, base, None) == base + timedelta(days=1)


def test_custom_cron_uses_croniter():
    base = datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)
    nxt = next_run_time(Recurrence.CUSTOM_CRON, base, "0 12 * * *")
    assert nxt == datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_none_returns_none():
    base = datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert next_run_time(Recurrence.NONE, base, None) is None
