"""
reconciliation.py — On reconnect, reconcile offline accounting buffer with master.

Triggered when NATS reconnects after an offline period.
Also handles quota sync: if master has different consumption numbers,
update local cache to match.
"""
from __future__ import annotations

import logging

import httpx

from app.aggregator import flush_buffered_events
from app.cache_loader import load_from_master
from app.config import settings

log = logging.getLogger(__name__)


async def reconcile() -> None:
    """
    Full reconciliation after reconnection:
    1. Flush all buffered accounting events.
    2. Re-load user/package cache from master.
    """
    log.info("Starting reconciliation after reconnect")

    # Step 1: Flush offline buffer
    flushed = await flush_buffered_events()
    log.info("Reconciliation: flushed offline events", count=flushed)

    # Step 2: Reload fresh data from master
    ok = await load_from_master()
    if ok:
        log.info("Reconciliation complete — cache refreshed")
    else:
        log.warning("Reconciliation: cache refresh failed, will retry")


async def report_heartbeat(active_sessions: int = 0) -> None:
    """Send heartbeat to master HTTP endpoint."""
    url = f"{settings.master_url}/internal/edge/heartbeat"
    headers = {"X-RADIUS-Token": settings.radius_internal_token}
    body = {
        "edge_id": settings.edge_id,
        "active_sessions": active_sessions,
        "nats_connected": True,
    }
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(url, json=body, headers=headers)
    except Exception as exc:
        log.debug("Heartbeat failed", error=str(exc))
