from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.db import init_db
from app.radius_api import router as radius_router

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────────────────────
    await init_db()

    # Connect to NATS
    from app import nats_client
    await nats_client.connect()

    # Load initial cache from master
    from app.cache_loader import load_from_master, periodic_refresh
    try:
        await load_from_master()
    except Exception as exc:
        log.warning("Initial cache load failed — running in offline mode", error=str(exc))

    # Start background tasks
    from app.aggregator import periodic_flush
    from app.reconciliation import report_heartbeat
    from app.coa_listener import start_coa_listener

    tasks = [
        asyncio.create_task(periodic_refresh(), name="cache-refresh"),
        asyncio.create_task(periodic_flush(), name="acct-flush"),
        asyncio.create_task(_heartbeat_loop(), name="heartbeat"),
        asyncio.create_task(start_coa_listener(), name="coa-listener"),
    ]

    yield

    # ── Shutdown ──────────────────────────────────────────────────────────────
    for task in tasks:
        task.cancel()
    await nats_client.close()


async def _heartbeat_loop() -> None:
    from app.reconciliation import report_heartbeat
    while True:
        await asyncio.sleep(settings.heartbeat_interval_seconds)
        try:
            await report_heartbeat()
        except Exception:
            pass


app = FastAPI(
    title="NetManager Edge Agent",
    version="0.2.0",
    docs_url="/docs" if settings.debug else None,
    lifespan=lifespan,
)

app.include_router(radius_router)


@app.get("/health")
async def health() -> dict:
    from app import nats_client
    return {
        "status": "ok",
        "edge_id": settings.edge_id,
        "nats_connected": nats_client.is_connected(),
    }

