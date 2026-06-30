from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.exception_handlers import register_exception_handlers
from app.middleware import register_middleware
from app.routers.tasks import router
from app.services.scheduler import shutdown, start
from app.utils.logging import configure_logging


def create_app(start_scheduler: bool = True) -> FastAPI:
    configure_logging()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if start_scheduler:
            start()
            try:
                yield
            finally:
                shutdown()
        else:
            yield

    app = FastAPI(title="Task Scheduler Service", lifespan=lifespan)
    register_exception_handlers(app)
    register_middleware(app)
    app.include_router(router)
    return app


app = create_app()
