"""Explicit environment mapping; never reads dotenv files."""

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    db_host: str = "postgres"
    db_port: int = 5432
    db_name: str = "hardware_catalog"
    db_user: str = "hardware_user"
    db_password: str = field(default="", repr=False)
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = field(default="", repr=False)
    minio_secret_key: str = field(default="", repr=False)

    @classmethod
    def from_environ(cls, env: Mapping[str, str]) -> "Settings":
        return cls(
            db_host=env.get("DB_HOST", "postgres"),
            db_port=int(env.get("DB_PORT", "5432")),
            db_name=env.get("DB_NAME", "hardware_catalog"),
            db_user=env.get("DB_USER", "hardware_user"),
            db_password=env.get("DB_PASSWORD", ""),
            minio_endpoint=env.get("MINIO_ENDPOINT", "http://minio:9000"),
            minio_access_key=env.get("MINIO_ACCESS_KEY", ""),
            minio_secret_key=env.get("MINIO_SECRET_KEY", ""),
        )
