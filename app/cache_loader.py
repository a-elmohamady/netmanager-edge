"""
cache_loader.py — Loads user/package data from master API on startup/reconnect.

Calls: POST /internal/edge/bootstrap
Populates local SQLite cache.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime

import httpx
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import AccountingBuffer, PackageCache, UserCache, async_session

log = logging.getLogger(__name__)


async def load_from_master() -> bool:
    """
    Fetch full snapshot from master API and replace local cache.
    Returns True on success.
    """
    url = f"{settings.master_url}/internal/edge/bootstrap"
    headers = {
        "X-RADIUS-Token": settings.radius_internal_token,
    }
    params = {"edge_id": settings.edge_id}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPError as exc:
        log.error("Failed to fetch bootstrap from master", error=str(exc))
        return False

    async with async_session() as db:
        async with db.begin():
            # Replace user cache
            await db.execute(delete(UserCache))
            for u in data.get("users", []):
                db.add(UserCache(
                    auth_code=u["auth_code"],
                    subscription_id=str(u.get("subscription_id", "")),
                    package_code=u.get("package_code") or "",
                    quota_total_bytes=u.get("quota_total_bytes"),
                    used_download_bytes=u.get("used_download_bytes", 0),
                    used_upload_bytes=u.get("used_upload_bytes", 0),
                    used_time_seconds=0,
                    rate_limit_down_kbps=u.get("rate_down_kbps"),
                    rate_limit_up_kbps=u.get("rate_up_kbps"),
                    status=u.get("status", "active"),
                    synced_at=datetime.now(UTC),
                ))

            # Replace package cache
            await db.execute(delete(PackageCache))
            for p in data.get("packages", []):
                db.add(PackageCache(
                    code=p["code"],
                    name=p["name"],
                    quota_total_bytes=p.get("quota_total_bytes"),
                    quota_time_seconds=p.get("quota_time_seconds"),
                    rate_limit_down_kbps=p.get("rate_down_kbps"),
                    rate_limit_up_kbps=p.get("rate_up_kbps"),
                    duration_days=p.get("duration_days"),
                    active=True,
                ))

    synced_at = data.get("synced_at", "")
    log.info(
        "Cache loaded from master",
        users=len(data.get("users", [])),
        packages=len(data.get("packages", [])),
        synced_at=synced_at,
    )
    return True


async def periodic_refresh() -> None:
    """Background task: refresh cache every N seconds."""
    import asyncio
    while True:
        await asyncio.sleep(settings.cache_refresh_interval_seconds)
        try:
            await load_from_master()
        except Exception as exc:
            log.error("Cache refresh failed", error=str(exc))
