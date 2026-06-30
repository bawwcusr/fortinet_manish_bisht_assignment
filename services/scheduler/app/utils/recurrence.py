from datetime import datetime, timedelta

from croniter import croniter

from app.utils.enums import Recurrence


def next_run_time(
    recurrence: Recurrence, base: datetime, cron_expr: str | None
) -> datetime | None:
    """Return the next execution_time after base, or None for one-shot (NONE)."""
    if recurrence == Recurrence.HOURLY:
        return base + timedelta(hours=1)
    if recurrence == Recurrence.DAILY:
        return base + timedelta(days=1)
    if recurrence == Recurrence.CUSTOM_CRON:
        return croniter(cron_expr, base).get_next(datetime)
    return None
