"""
coa_listener.py — Listens for CoA commands from master via NATS JetStream.

Master publishes to: nm.coa.{edge_id}
Commands:
  - disconnect: Disconnect a session (sends Disconnect-Request to NAS)
  - rate_change: Change rate limits (sends CoA-Request to NAS)
  - cache_invalidate: Refresh a specific user in local cache
"""
from __future__ import annotations

import asyncio
import json
import logging
import socket
import struct
from typing import Optional

from app.config import settings
from app.nats_client import _nc, _js

log = logging.getLogger(__name__)


async def start_coa_listener() -> None:
    """Subscribe to NATS CoA subject and process commands."""
    import nats
    if not _js:
        log.warning("NATS not connected — CoA listener not started")
        return

    subject = f"nm.coa.{settings.edge_id}"
    try:
        sub = await _js.subscribe(subject, durable=f"edge-coa-{settings.edge_id}")
        log.info("CoA listener started", subject=subject)
        async for msg in sub.messages:
            await _handle_coa_message(msg)
    except Exception as exc:
        log.error("CoA listener error", error=str(exc))


async def _handle_coa_message(msg) -> None:
    try:
        data = json.loads(msg.data)
        command = data.get("command")

        if command == "disconnect":
            await _do_disconnect(
                nas_ip=data["nas_ip"],
                nas_port=data.get("coa_port", 3799),
                secret=data["shared_secret"],
                acct_session_id=data["acct_session_id"],
            )
        elif command == "rate_change":
            await _do_rate_change(
                nas_ip=data["nas_ip"],
                nas_port=data.get("coa_port", 3799),
                secret=data["shared_secret"],
                acct_session_id=data["acct_session_id"],
                rate_down=data["rate_down_kbps"],
                rate_up=data["rate_up_kbps"],
            )
        elif command == "cache_invalidate":
            auth_code = data.get("auth_code")
            if auth_code:
                await _invalidate_user_cache(auth_code)

        await msg.ack()
    except Exception as exc:
        log.error("Failed to handle CoA message", error=str(exc))
        await msg.nak()


async def _do_disconnect(
    nas_ip: str,
    nas_port: int,
    secret: str,
    acct_session_id: str,
) -> None:
    """Send RADIUS Disconnect-Request to NAS."""
    import hashlib
    import os

    DISCONNECT_REQUEST = 40
    attrs = _encode_attr(44, acct_session_id.encode())  # Acct-Session-Id
    secret_b = secret.encode()
    auth = os.urandom(16)
    length = 20 + len(attrs)
    header = struct.pack("BBH16s", DISCONNECT_REQUEST, 1, length, auth)
    md5 = hashlib.md5(header + attrs + secret_b).digest()
    packet = struct.pack("BBH16s", DISCONNECT_REQUEST, 1, length, md5) + attrs

    await _udp_send(nas_ip, nas_port, packet)
    log.info("Sent Disconnect-Request", nas=nas_ip, session=acct_session_id)


async def _do_rate_change(
    nas_ip: str,
    nas_port: int,
    secret: str,
    acct_session_id: str,
    rate_down: int,
    rate_up: int,
) -> None:
    """Send RADIUS CoA-Request with new rate limits."""
    import hashlib
    import os

    COA_REQUEST = 43
    rate_str = f"{rate_down}k/{rate_up}k".encode()
    mikrotik_vsa = struct.pack(">I", 14988) + struct.pack("BB", 8, 2 + len(rate_str)) + rate_str
    attrs = _encode_attr(26, mikrotik_vsa) + _encode_attr(44, acct_session_id.encode())
    secret_b = secret.encode()
    auth = os.urandom(16)
    length = 20 + len(attrs)
    header = struct.pack("BBH16s", COA_REQUEST, 1, length, auth)
    md5 = hashlib.md5(header + attrs + secret_b).digest()
    packet = struct.pack("BBH16s", COA_REQUEST, 1, length, md5) + attrs

    await _udp_send(nas_ip, nas_port, packet)
    log.info("Sent CoA rate change", nas=nas_ip, down=rate_down, up=rate_up)


async def _invalidate_user_cache(auth_code: str) -> None:
    """Remove user from local SQLite cache, forcing re-auth against master."""
    from sqlalchemy import delete
    from app.db import UserCache, async_session

    async with async_session() as db:
        await db.execute(delete(UserCache).where(UserCache.auth_code == auth_code))
        await db.commit()
    log.info("Cache invalidated for user", auth_code=auth_code)


def _encode_attr(attr_type: int, value: bytes) -> bytes:
    return struct.pack("BB", attr_type, 2 + len(value)) + value


async def _udp_send(host: str, port: int, data: bytes) -> None:
    loop = asyncio.get_event_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    try:
        await loop.sock_sendto(sock, data, (host, port))
    except Exception as exc:
        log.warning("UDP send failed", host=host, error=str(exc))
    finally:
        sock.close()
