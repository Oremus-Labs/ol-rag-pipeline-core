from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pipeline_version: str = Field(alias="PIPELINE_VERSION")
    dataset_version: str = Field(alias="DATASET_VERSION")

    pg_dsn: str | None = Field(default=None, alias="PG_DSN")

    qdrant_url: str = Field(alias="QDRANT_URL")
    qdrant_api_key: str | None = Field(default=None, alias="QDRANT_API_KEY")
    qdrant_collection: str = Field(default="library_chunks_v1", alias="QDRANT_COLLECTION")

    s3_endpoint: str = Field(alias="S3_ENDPOINT")
    s3_bucket: str = Field(alias="S3_BUCKET")

    nats_url: str = Field(alias="NATS_URL")


def load_settings() -> Settings:
    return Settings()
