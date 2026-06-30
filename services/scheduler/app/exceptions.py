from app.utils.enums import TaskStatus


class AppException(Exception):
    """Domain errors mapped to HTTP responses by root handlers."""

    def __init__(self, message: str, *, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class TaskNotFound(AppException):
    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__("task not found", status_code=404)


class InvalidCancel(AppException):
    def __init__(self, status: TaskStatus):
        self.status = status
        super().__init__("task already terminal", status_code=409)


class InvalidTransition(AppException):
    def __init__(self, message: str):
        super().__init__(message, status_code=422)


class JobRegistrationError(AppException):
    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__("failed to schedule task", status_code=503)


class InvalidWebhook(AppException):
    def __init__(self, message: str):
        super().__init__(message, status_code=422)
