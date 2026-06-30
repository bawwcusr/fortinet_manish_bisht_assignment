from fastapi import FastAPI

from app.exception_handlers import register_exception_handlers
from app.middleware import register_middleware
from app.routers.webhooks import router
from app.utils.logging import configure_logging


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="Task Executor Service")
    register_exception_handlers(app)
    register_middleware(app)
    app.include_router(router)
    return app


app = create_app()
