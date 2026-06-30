from app.exceptions import InvalidTransition
from app.utils.enums import TaskStatus

# Valid status edges. RUNNING→RUNNING covers async 202: webhook accepted, poll pending.
_ALLOWED = {
    TaskStatus.CREATED: {TaskStatus.PENDING, TaskStatus.CANCELLED},
    TaskStatus.PENDING: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.RUNNING},
    TaskStatus.SUCCESS: set(),  # terminal
    TaskStatus.FAILED: set(),  # terminal
    TaskStatus.CANCELLED: set(),  # terminal
}


def can_transition(src: TaskStatus, dst: TaskStatus) -> bool:
    return dst in _ALLOWED.get(src, set())


def assert_transition(src: TaskStatus, dst: TaskStatus) -> None:
    if not can_transition(src, dst):
        raise InvalidTransition(f"{src} -> {dst} not allowed")
