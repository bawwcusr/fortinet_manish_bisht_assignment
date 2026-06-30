from datetime import datetime, timezone
from app.models.task import Task
from app.utils.enums import TaskStatus, Recurrence
from app.services import scheduler as sched


def test_register_and_cancel_job_uses_memory_store():
    sched.init_scheduler(in_memory=True)
    task = Task(
        id="abc",
        name="t",
        execution_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
        webhook_url="http://e/x",
        payload={},
        recurrence=Recurrence.NONE,
        status=TaskStatus.PENDING,
        max_retries=3,
    )
    sched.register_task_job(task)
    assert sched.get_scheduler().get_job("abc") is not None
    sched.remove_task_job("abc")
    assert sched.get_scheduler().get_job("abc") is None
