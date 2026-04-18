from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.db import init_db
from app.radius_api import router as radius_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    # TODO: start NATS connection, cache loader, aggregator
    yield


app = FastAPI(
    title="NetManager Edge Agent",
    version="0.1.0",
    docs_url="/docs" if settings.debug else None,
    lifespan=lifespan,
)

app.include_router(radius_router)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "edge_id": settings.edge_id}
