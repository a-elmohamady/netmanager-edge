from __future__ import annotations

import hmac
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request, status
from pydantic import BaseModel

from app.config import settings
from app.db import AsyncSessionLocal, UserCache
from sqlalchemy import select

router = APIRouter(prefix="/radius")


def _verify_token(token: str | None) -> None:
    """Verify the shared internal RADIUS token."""
    if not token or not hmac.compare_digest(token, settings.radius_internal_token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid RADIUS token")


class AuthorizeRequest(BaseModel):
    username: str
    password: str | None = None
    nas_ip_address: str | None = None
    called_station_id: str | None = None
    calling_station_id: str | None = None
    nas_port: int | None = None


class AuthorizeResponse(BaseModel):
    result: str  # "accept" | "reject"
    reply_attributes: dict[str, Any] = {}


@router.post("/authorize", response_model=AuthorizeResponse)
async def authorize(
    req: AuthorizeRequest,
    x_radius_token: str | None = Header(None, alias="X-RADIUS-Token"),
) -> AuthorizeResponse:
    _verify_token(x_radius_token)

    async with AsyncSessionLocal() as db:
        result = await db.execute(select(UserCache).where(UserCache.auth_code == req.username))
        user = result.scalar_one_or_none()

    if not user or user.status != "active":
        return AuthorizeResponse(result="reject")

    # Check quota
    if user.quota_total_bytes and (
        user.used_download_bytes + user.used_upload_bytes >= user.quota_total_bytes
    ):
        return AuthorizeResponse(result="reject", reply_attributes={"Reply-Message": "Quota exhausted"})

    reply: dict[str, Any] = {}
    if user.rate_limit_down_kbps and user.rate_limit_up_kbps:
        reply["Mikrotik-Rate-Limit"] = f"{user.rate_limit_down_kbps}k/{user.rate_limit_up_kbps}k"
    if user.quota_total_bytes:
        remaining = user.quota_total_bytes - (user.used_download_bytes + user.used_upload_bytes)
        reply["Mikrotik-Total-Limit"] = str(max(0, remaining))
    reply["Acct-Interim-Interval"] = "60"

    return AuthorizeResponse(result="accept", reply_attributes=reply)


class AccountingRequest(BaseModel):
    acct_status_type: str  # Start / Interim-Update / Stop
    acct_session_id: str
    username: str
    acct_input_octets: int = 0
    acct_output_octets: int = 0
    acct_session_time: int = 0
    framed_ip_address: str | None = None
    calling_station_id: str | None = None


@router.post("/accounting", status_code=status.HTTP_204_NO_CONTENT)
async def accounting(
    req: AccountingRequest,
    x_radius_token: str | None = Header(None, alias="X-RADIUS-Token"),
) -> None:
    _verify_token(x_radius_token)

    from app.db import AccountingBuffer
    now = datetime.utcnow()

    async with AsyncSessionLocal() as db:
        event = AccountingBuffer(
            acct_session_id=req.acct_session_id,
            auth_code=req.username,
            event_type=req.acct_status_type.lower().replace("-", "_"),
            input_octets=req.acct_input_octets,
            output_octets=req.acct_output_octets,
            session_time=req.acct_session_time,
            window_start=now,
            window_end=now,
            framed_ip=req.framed_ip_address,
            mac=req.calling_station_id,
        )
        db.add(event)
        await db.commit()
