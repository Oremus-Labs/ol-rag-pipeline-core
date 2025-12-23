from __future__ import annotations

from dataclasses import dataclass

import boto3
from botocore.config import Config


@dataclass(frozen=True)
class S3Config:
    endpoint: str
    bucket: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"


class S3Client:
    def __init__(self, cfg: S3Config):
        self._cfg = cfg
        self._client = boto3.client(
            "s3",
            endpoint_url=cfg.endpoint,
            aws_access_key_id=cfg.access_key,
            aws_secret_access_key=cfg.secret_key,
            region_name=cfg.region,
            config=Config(s3={"addressing_style": "path"}),
        )

    @property
    def bucket(self) -> str:
        return self._cfg.bucket

    def head(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._cfg.bucket, Key=key)
            return True
        except Exception:  # noqa: BLE001
            return False

    def get_bytes(self, key: str) -> bytes:
        obj = self._client.get_object(Bucket=self._cfg.bucket, Key=key)
        return obj["Body"].read()

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
    ) -> str:
        extra: dict = {}
        if content_type:
            extra["ContentType"] = content_type
        self._client.put_object(Bucket=self._cfg.bucket, Key=key, Body=data, **extra)
        return f"s3://{self._cfg.bucket}/{key}"

