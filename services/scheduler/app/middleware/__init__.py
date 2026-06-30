from fastapi import FastAPI

from app.middleware.logging import RequestLoggingMiddleware
from app.middleware.request_id import RequestIDMiddleware


def register_middleware(app: FastAPI) -> None:
    # Starlette runs middleware in reverse add order on incoming requests.
    app.add_middleware(RequestLoggingMiddleware)
    app.add_middleware(RequestIDMiddleware)
