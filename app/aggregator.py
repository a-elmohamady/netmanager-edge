"""
aggregator.py — Aggregates accounting events into 5-minute windows
and flushes them to master via NATS or HTTP fallback.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import select, update

from app.config import settings
from app.db import AccountingBuffer, async_session
from app.nats_client import is_connected, publish_accounting

log = logging.getLogger(__name__)


async def flush_buffered_events() -> int:
    """
    Flush un-sent accounting events to master via NATS.
    Falls back to direct HTTP if NATS is offline.
    Returns number of events flushed.
    """
    async with async_session() as db:
        result = await db.execute(
            select(AccountingBuffer)
            .where(AccountingBuffer.flushed.is_(False))
            .order_by(AccountingBuffer.window_start.asc())
            .limit(500)
        )
        events = result.scalars().all()
        if not events:
            return 0

        flushed = 0
        for evt in events:
            payload = {
                "acct_status_type": evt.event_type,
                "acct_session_id": evt.acct_session_id,
                "username": evt.auth_code,
                "acct_input_octets": evt.input_octets,
                "acct_output_octets": evt.output_octets,
                "acct_session_time": evt.session_time,
                "edge_id": settings.edge_id,
                "tenant_id": settings.tenant_id,
                "window_start": evt.window_start.isoformat(),
                "window_end": evt.window_end.isoformat(),
                "framed_ip": evt.framed_ip,
                "mac": evt.mac,
            }

            success = await publish_accounting(payload)
            if not success:
                # HTTP fallback
                success = await _http_fallback(payload)

            if success:
                await db.execute(
                    update(AccountingBuffer)
                    .where(AccountingBuffer.id == evt.id)
                    .values(flushed=True)
                )
                flushed += 1
            else:
                break  # Stop if both NATS and HTTP fail (offline)

        await db.commit()
        return flushed


async def _http_fallback(payload: dict) -> bool:
    """Send accounting event directly to master HTTP API."""
    import httpx
    url = f"{settings.master_url}/internal/radius/accounting"
    headers = {"X-RADIUS-Token": settings.radius_internal_token}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload, headers=headers)
            return resp.status_code == 204
    except Exception as exc:
        log.warning("HTTP fallback failed", error=str(exc))
        return False


async def periodic_flush() -> None:
    """Background task: flush every N seconds."""
    while True:
        await asyncio.sleep(settings.accounting_flush_interval_seconds)
        try:
            n = await flush_buffered_events()
            if n:
                log.info("Flushed accounting events", count=n)
        except Exception as exc:
            log.error("Flush failed", error=str(exc))
