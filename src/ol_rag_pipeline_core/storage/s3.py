from __future__ import annotations

from dataclasses import dataclass

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


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

    def delete_key(self, key: str) -> None:
        # AWS S3 delete_object is idempotent; MinIO behaves similarly. Treat missing keys as ok.
        try:
            self._client.delete_object(Bucket=self._cfg.bucket, Key=key)
        except ClientError as e:  # pragma: no cover
            # Defensive: some S3-compatible gateways can return NoSuchKey as an error.
            code = (e.response or {}).get("Error", {}).get("Code")
            if code in {"NoSuchKey", "NotFound"}:
                return
            raise

    def delete_uri(self, uri: str) -> None:
        bucket, key = parse_s3_uri(uri)
        if bucket != self._cfg.bucket:
            raise ValueError(f"Bucket mismatch for URI: {uri}")
        self.delete_key(key)

    def list_keys(self, *, prefix: str) -> list[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self._cfg.bucket, Prefix=prefix):
            for obj in page.get("Contents") or []:
                k = obj.get("Key")
                if isinstance(k, str) and k:
                    keys.append(k)
        return keys

    def delete_prefix(self, *, prefix: str) -> int:
        """
        Delete all objects under `prefix`. Returns number of keys attempted.
        """
        keys = self.list_keys(prefix=prefix)
        if not keys:
            return 0

        deleted = 0
        for i in range(0, len(keys), 1000):
            chunk = keys[i : i + 1000]
            self._client.delete_objects(
                Bucket=self._cfg.bucket,
                Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
            )
            deleted += len(chunk)
        return deleted
