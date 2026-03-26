from __future__ import annotations

import os
from typing import Any

from pydantic import BaseModel, SecretStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ExtraS3Source(BaseModel):
    secret_name: str
    key_id: str
    secret: SecretStr
    region: str
    scope: str


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    APP_NAME: str = "ParquetFederation"
    DEBUG: bool = False

    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: SecretStr | None = None
    AWS_REGION: str = "us-east-1"
    S3_BUCKET_NAME: str = ""

    EXTRA_S3_SOURCES: list[ExtraS3Source] = []

    FEDERATION_PG_DSN: SecretStr | None = None

    DUCKDB_MEMORY_LIMIT: str = "4GB"
    DUCKDB_THREADS: int = Field(default_factory=lambda: os.cpu_count() or 4)
    DUCKDB_SPILL_DIR: str = "/tmp/duckdb_spill"
    DUCKDB_POOL_SIZE: int = 10

    DEFAULT_QUERY_TIMEOUT: int = 30
    MAX_QUERY_TIMEOUT: int = 300
    MAX_RESULT_ROWS: int = 100_000


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
