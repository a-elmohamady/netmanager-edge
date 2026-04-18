from __future__ import annotations

from sqlalchemy import Column, Integer, String, BigInteger, Boolean, Float, DateTime, Text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from datetime import datetime

from app.config import settings


class Base(DeclarativeBase):
    pass


class UserCache(Base):
    """Active users with their quota snapshots — no PII."""
    __tablename__ = "users_cache"

    auth_code: str = Column(String, primary_key=True)
    subscription_id: str = Column(String, nullable=False)
    package_code: str = Column(String, nullable=False)
    # Quota
    quota_download_bytes: int | None = Column(BigInteger, nullable=True)
    quota_upload_bytes: int | None = Column(BigInteger, nullable=True)
    quota_total_bytes: int | None = Column(BigInteger, nullable=True)
    used_download_bytes: int = Column(BigInteger, default=0, nullable=False)
    used_upload_bytes: int = Column(BigInteger, default=0, nullable=False)
    used_time_seconds: int = Column(BigInteger, default=0, nullable=False)
    # Speed
    rate_limit_down_kbps: int | None = Column(Integer, nullable=True)
    rate_limit_up_kbps: int | None = Column(Integer, nullable=True)
    # Auth (temp credentials for captive sessions, NOT PPPoE)
    temp_username: str | None = Column(String, nullable=True)
    temp_password_hash: str | None = Column(String, nullable=True)
    temp_expires_at: datetime | None = Column(DateTime, nullable=True)
    # Status
    status: str = Column(String, default="active", nullable=False)
    synced_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)


class PackageCache(Base):
    __tablename__ = "packages_cache"

    code: str = Column(String, primary_key=True)
    name: str = Column(String, nullable=False)
    quota_total_bytes: int | None = Column(BigInteger, nullable=True)
    quota_time_seconds: int | None = Column(BigInteger, nullable=True)
    rate_limit_down_kbps: int | None = Column(Integer, nullable=True)
    rate_limit_up_kbps: int | None = Column(Integer, nullable=True)
    duration_days: int | None = Column(Integer, nullable=True)
    active: bool = Column(Boolean, default=True, nullable=False)


class AccountingBuffer(Base):
    """Accounting events not yet sent to master."""
    __tablename__ = "accounting_buffer"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    acct_session_id: str = Column(String, nullable=False)
    auth_code: str = Column(String, nullable=False)
    event_type: str = Column(String, nullable=False)  # start/interim/stop
    input_octets: int = Column(BigInteger, default=0, nullable=False)
    output_octets: int = Column(BigInteger, default=0, nullable=False)
    session_time: int = Column(Integer, default=0, nullable=False)
    window_start: datetime = Column(DateTime, nullable=False)
    window_end: datetime = Column(DateTime, nullable=False)
    framed_ip: str | None = Column(String, nullable=True)
    mac: str | None = Column(String, nullable=True)
    flushed: bool = Column(Boolean, default=False, nullable=False)
    created_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)


class LiveSessionLocal(Base):
    __tablename__ = "sessions_live"

    acct_session_id: str = Column(String, primary_key=True)
    auth_code: str = Column(String, nullable=False)
    started_at: datetime = Column(DateTime, nullable=False)
    last_interim_at: datetime | None = Column(DateTime, nullable=True)
    input_octets: int = Column(BigInteger, default=0)
    output_octets: int = Column(BigInteger, default=0)
    framed_ip: str | None = Column(String)
    mac: str | None = Column(String)


engine = create_async_engine(
    f"sqlite+aiosqlite:///{settings.sqlite_path}",
    echo=settings.debug,
)
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session():
    async with AsyncSessionLocal() as session:
        yield session
