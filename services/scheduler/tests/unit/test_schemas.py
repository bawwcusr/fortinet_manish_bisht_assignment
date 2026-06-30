import pytest
from datetime import datetime, timezone
from app.models.schemas import TaskCreate
from app.utils.enums import Recurrence


def test_invalid_cron_rejected():
    with pytest.raises(ValueError):
        TaskCreate(
            name="x",
            execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
            webhook_url="http://e/x",
            payload={},
            recurrence=Recurrence.CUSTOM_CRON,
            cron_expr="not a cron",
        )


def test_valid_cron_accepted():
    dto = TaskCreate(
        name="x",
        execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
        webhook_url="http://e/x",
        payload={},
        recurrence=Recurrence.CUSTOM_CRON,
        cron_expr="0 12 * * *",
    )
    assert dto.cron_expr == "0 12 * * *"
