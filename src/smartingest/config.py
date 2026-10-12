"""Runtime configuration from environment variables."""

from __future__ import annotations

import os
from functools import lru_cache

from pydantic import BaseModel

from smartingest.security import UploadLimits


def _env_bool(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes")


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


class Settings(BaseModel):
    database_url: str
    # Bronze storage: "local" (a folder / Docker volume) or "s3" (any S3-compatible service)
    bronze_backend: str = "local"
    bronze_local_path: str = "/data/bronze"
    s3_endpoint: str | None = None        # e.g. s3.amazonaws.com or <account>.r2.cloudflarestorage.com
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_secure: bool = True
    bronze_bucket: str = "bronze"
    auto_apply_high: bool = True

    # Access
    admin_api_key: str | None = None      # full access for the owner
    demo_mode: bool = True                # anonymous visitors may create demo sessions

    # Demo visitors
    demo_session_ttl_hours: int = 24      # all demo data is deleted after this
    demo_max_uploads_per_session: int = 10
    demo_sessions_per_ip_per_hour: int = 5
    demo_limits: UploadLimits = UploadLimits(max_bytes=5 * 1024 * 1024, max_rows=5_000, max_columns=50)
    admin_limits: UploadLimits = UploadLimits(max_bytes=20 * 1024 * 1024, max_rows=100_000, max_columns=200)

    # Abuse and cost protection
    requests_per_minute_per_ip: int = 60
    llm_daily_call_limit: int = 300       # across all users; protects the API bill
    llm_items_per_upload: int = 100       # invalid values sent to Claude per upload
    demo_llm_enabled: bool = True

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ
        return cls(
            database_url=env["DATABASE_URL"],
            bronze_backend=env.get("BRONZE_BACKEND", "local").strip().lower(),
            bronze_local_path=env.get("BRONZE_LOCAL_PATH", "/data/bronze"),
            s3_endpoint=env.get("S3_ENDPOINT") or None,
            s3_access_key=env.get("S3_ACCESS_KEY") or None,
            s3_secret_key=env.get("S3_SECRET_KEY") or None,
            s3_secure=_env_bool("S3_SECURE", True),
            bronze_bucket=env.get("BRONZE_BUCKET", "bronze"),
            auto_apply_high=_env_bool("AUTO_APPLY_HIGH", True),
            admin_api_key=env.get("ADMIN_API_KEY") or None,
            demo_mode=_env_bool("DEMO_MODE", True),
            demo_session_ttl_hours=_env_int("DEMO_SESSION_TTL_HOURS", 24),
            demo_max_uploads_per_session=_env_int("DEMO_MAX_UPLOADS_PER_SESSION", 10),
            demo_sessions_per_ip_per_hour=_env_int("DEMO_SESSIONS_PER_IP_PER_HOUR", 5),
            demo_limits=UploadLimits(
                max_bytes=_env_int("DEMO_MAX_UPLOAD_MB", 5) * 1024 * 1024,
                max_rows=_env_int("DEMO_MAX_ROWS", 5_000),
                max_columns=_env_int("DEMO_MAX_COLUMNS", 50),
            ),
            admin_limits=UploadLimits(
                max_bytes=_env_int("ADMIN_MAX_UPLOAD_MB", 20) * 1024 * 1024,
                max_rows=_env_int("ADMIN_MAX_ROWS", 100_000),
                max_columns=_env_int("ADMIN_MAX_COLUMNS", 200),
            ),
            requests_per_minute_per_ip=_env_int("REQUESTS_PER_MINUTE_PER_IP", 60),
            llm_daily_call_limit=_env_int("LLM_DAILY_CALL_LIMIT", 300),
            llm_items_per_upload=_env_int("LLM_ITEMS_PER_UPLOAD", 100),
            demo_llm_enabled=_env_bool("DEMO_LLM_ENABLED", True),
        )

    def check(self) -> None:
        """Refuse to start with an unsafe configuration."""
        if self.admin_api_key is not None and len(self.admin_api_key) < 32:
            raise RuntimeError("ADMIN_API_KEY must be at least 32 characters (use: openssl rand -hex 32)")
        if "change-me" in self.database_url:
            raise RuntimeError("APP_DB_PASSWORD still has the example value; set a strong value in .env")
        if self.bronze_backend not in ("local", "s3"):
            raise RuntimeError("BRONZE_BACKEND must be 'local' or 's3'")
        if self.bronze_backend == "s3" and not (self.s3_endpoint and self.s3_access_key and self.s3_secret_key):
            raise RuntimeError("BRONZE_BACKEND=s3 needs S3_ENDPOINT, S3_ACCESS_KEY and S3_SECRET_KEY")


@lru_cache
def get_settings() -> Settings:
    settings = Settings.from_env()
    settings.check()
    return settings
