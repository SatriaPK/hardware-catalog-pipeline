"""Immutable conditional S3 writes and verified reads, without ETag assumptions."""

import gzip
import hashlib
import re
from typing import Any

from botocore.exceptions import ClientError


class IntegrityConflict(RuntimeError):
    pass


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def safe_segment(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value):
        raise ValueError("Unsafe object identity")
    return value


class ObjectStore:
    def __init__(self, client: Any, *, label: str = "TEST_ONLY"):
        self.client = client
        if label not in {"TEST_ONLY", "REAL_API"}:
            raise ValueError("Unknown data label")
        self.label = label

    def put(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str,
        metadata: dict[str, str] | None = None,
    ) -> str:
        checksum = digest(body)
        metadata = {**(metadata or {}), "sha256": checksum, "label": self.label}
        try:
            self.client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentType=content_type,
                Metadata=metadata,
                IfNoneMatch="*",
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
                raise
        self.read(bucket, key, checksum)
        return checksum

    def read(self, bucket: str, key: str, expected: str) -> bytes:
        head = self.client.head_object(Bucket=bucket, Key=key)
        response = self.client.get_object(Bucket=bucket, Key=key)
        stream = response["Body"]
        try:
            body = stream.read()
        finally:
            stream.close()
        if (
            digest(body) != expected
            or head["Metadata"].get("sha256") != expected
            or head["ContentLength"] != len(body)
        ):
            raise IntegrityConflict("Object checksum conflict; overwrite refused")
        return body

    def read_raw(self, bucket: str, key: str, payload_hash: str) -> bytes:
        head = self.client.head_object(Bucket=bucket, Key=key)
        if head["Metadata"].get("content-sha256") != payload_hash:
            raise IntegrityConflict("RAW provenance checksum conflict")
        compressed = self.read(bucket, key, head["Metadata"].get("sha256", ""))
        try:
            body = gzip.decompress(compressed)
        except (OSError, EOFError) as error:
            raise IntegrityConflict("Corrupt RAW gzip") from error
        if digest(body) != payload_hash:
            raise IntegrityConflict("RAW payload checksum conflict")
        return body
