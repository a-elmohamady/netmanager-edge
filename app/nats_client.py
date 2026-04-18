"""
nats_client.py — NATS JetStream client for edge agent.

Publishes accounting events to master via NATS JetStream.
Subject: nm.accounting.{tenant_id}
"""
from __future__ import annotations

import json
import logging
from typing import Optional

import nats
from nats.aio.client import Client
from nats.js import JetStreamContext

from app.config import settings

log = logging.getLogger(__name__)

_nc: Optional[Client] = None
_js: Optional[JetStreamContext] = None


async def connect() -> None:
    global _nc, _js
    try:
        _nc = await nats.connect(
            settings.nats_url,
            name=f"edge-{settings.edge_id}",
            reconnect_time_wait=2,
            max_reconnect_attempts=-1,  # infinite
            error_cb=_on_error,
            disconnected_cb=_on_disconnect,
            reconnected_cb=_on_reconnect,
        )
        _js = _nc.jetstream()
        log.info("NATS connected", server=settings.nats_url)
    except Exception as exc:
        log.warning("NATS connection failed, running offline", error=str(exc))
        _nc = None
        _js = None


async def close() -> None:
    global _nc
    if _nc:
        await _nc.close()
        _nc = None


def is_connected() -> bool:
    return _nc is not None and _nc.is_connected


async def publish_accounting(event: dict) -> bool:
    """
    Publish an accounting event to NATS JetStream.
    Returns True on success, False if offline.
    """
    if not _js:
        return False

    subject = f"nm.accounting.{settings.tenant_id}"
    payload = json.dumps(event, default=str).encode()
    try:
        ack = await _js.publish(subject, payload)
        log.debug("Published accounting event", seq=ack.seq)
        return True
    except Exception as exc:
        log.warning("Failed to publish to NATS", error=str(exc))
        return False


async def publish_heartbeat(stats: dict) -> None:
    """Publish edge heartbeat to master."""
    if not _js:
        return
    subject = f"nm.edge.heartbeat.{settings.edge_id}"
    payload = json.dumps(stats, default=str).encode()
    try:
        await _js.publish(subject, payload)
    except Exception:
        pass


async def _on_error(exc: Exception) -> None:
    log.error("NATS error", error=str(exc))


async def _on_disconnect() -> None:
    log.warning("NATS disconnected — switching to offline mode")


async def _on_reconnect() -> None:
    global _js
    if _nc:
        _js = _nc.jetstream()
    log.info("NATS reconnected")
