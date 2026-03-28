from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from parquet_federation.api.dependencies import init_pool, shutdown_pool
from parquet_federation.api.v1.router import router as v1_router
from parquet_federation.config import get_settings
from parquet_federation.exceptions import FederationError, QueryTimeoutError, QueryValidationError
from parquet_federation.middleware import RequestIdMiddleware

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings = get_settings()
    init_pool(settings)
    yield
    shutdown_pool()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.APP_NAME,
        version="1.0.0",
        description="DuckDB-powered data federation: JOIN S3 Parquet + PostgreSQL + HTTP CSV in one SQL query",
        lifespan=lifespan,
    )
    app.add_middleware(RequestIdMiddleware)
    app.include_router(v1_router)

    @app.exception_handler(QueryTimeoutError)
    async def timeout_handler(request, exc: QueryTimeoutError) -> JSONResponse:
        return JSONResponse(status_code=408, content={"detail": str(exc)})

    @app.exception_handler(QueryValidationError)
    async def validation_handler(request, exc: QueryValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(FederationError)
    async def federation_handler(request, exc: FederationError) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": str(exc)})

    @app.get("/")
    async def root() -> dict[str, str]:
        return {
            "service": settings.APP_NAME,
            "version": "1.0.0",
            "docs": "/docs",
        }

    return app


app = create_app()
