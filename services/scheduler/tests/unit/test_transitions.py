import pytest
from app.utils.enums import TaskStatus
from app.exceptions import InvalidTransition
from app.utils.transitions import assert_transition, can_transition


def test_allows_pending_to_running():
    assert can_transition(TaskStatus.PENDING, TaskStatus.RUNNING) is True


def test_disallows_success_to_running():
    assert can_transition(TaskStatus.SUCCESS, TaskStatus.RUNNING) is False


def test_assert_transition_raises_on_invalid():
    with pytest.raises(InvalidTransition):
        assert_transition(TaskStatus.SUCCESS, TaskStatus.PENDING)
