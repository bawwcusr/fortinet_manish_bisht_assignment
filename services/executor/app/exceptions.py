class AppException(Exception):
    def __init__(self, message: str, *, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class ExecutionNotFound(AppException):
    def __init__(self, execution_id: str):
        self.execution_id = execution_id
        super().__init__("not found", status_code=404)


class WebhookNotFound(AppException):
    def __init__(self, name: str):
        self.name = name
        super().__init__(f"unknown webhook: {name}", status_code=404)
