from __future__ import annotations

from pydantic import Field, RedisDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class EdgeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Edge identity
    edge_id: str
    edge_secret: str  # mTLS cert or shared secret with master
    tenant_id: str

    # Master connection
    master_url: str = "https://api.netmanager.local"
    nats_url: str = "nats://nats:4222"

    # Local storage
    sqlite_path: str = "/data/edge.db"
    redis_url: RedisDsn = "redis://redis:6379/0"  # type: ignore[assignment]

    # RADIUS internal token (must match FreeRADIUS config)
    radius_internal_token: str

    # Sync intervals
    cache_refresh_interval_seconds: int = 300  # 5 min
    accounting_flush_interval_seconds: int = 300
    heartbeat_interval_seconds: int = 30

    # Offline mode
    max_offline_hours: int = 24

    debug: bool = False
    log_level: str = "INFO"


settings = EdgeSettings()  # type: ignore[call-arg]
