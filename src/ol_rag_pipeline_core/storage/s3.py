from __future__ import annotations

from dataclasses import dataclass

import boto3
from botocore.config import Config


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Not an s3:// URI: {uri}")
    rest = uri.removeprefix("s3://")
    if "/" not in rest:
        raise ValueError(f"Invalid s3:// URI (missing key): {uri}")
    bucket, key = rest.split("/", 1)
    if not bucket or not key:
        raise ValueError(f"Invalid s3:// URI: {uri}")
    return bucket, key


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

    def get_bytes_uri(self, uri: str) -> bytes:
        bucket, key = parse_s3_uri(uri)
        if bucket != self._cfg.bucket:
            raise ValueError(f"Bucket mismatch for URI: {uri}")
        return self.get_bytes(key)

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
