"""FastAPI application factory for the IDRD backend."""

from __future__ import annotations

from fastapi import FastAPI

from idrd.interfaces.api.router import router


def create_app() -> FastAPI:
    app = FastAPI(
        title="IDRD Pipeline API",
        version="0.1.0",
        description="Async HTTP API for running and monitoring the IDRD dataset mention pipeline.",
    )
    app.include_router(router)
    return app


app = create_app()
