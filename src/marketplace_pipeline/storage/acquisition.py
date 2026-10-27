"""Explicitly owned acquisition namespace; never adopt fixture/public/user schemas."""

import re
from contextlib import contextmanager

from alembic import command
from alembic.config import Config
from botocore.exceptions import ClientError
from sqlalchemy import text

OWNER = "marketpipe:acquisition:v1"


class BucketSetupError(RuntimeError):
    def __init__(self, bucket: str, reason: str):
        self.bucket, self.reason = bucket, reason
        super().__init__(reason)


def bucket_tags(client, bucket):
    try:
        return client.get_bucket_tagging(Bucket=bucket)["TagSet"]
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") == "NoSuchTagSet":
            return []
        raise


def initialize_bucket(client, bucket, namespace):
    marker = {"Key": "marketpipe-acquisition", "Value": namespace}
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        if error.response["Error"]["Code"] not in {"404", "NoSuchBucket"}:
            raise
        client.create_bucket(Bucket=bucket)
        client.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": [marker]})
        return
    tags = bucket_tags(client, bucket)
    if marker in tags:
        return
    if tags:
        raise BucketSetupError(bucket, "existing_bucket_tags_do_not_match_namespace")
    current = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    versions = client.list_object_versions(Bucket=bucket, MaxKeys=1)
    if current.get("Contents") or versions.get("Versions") or versions.get("DeleteMarkers"):
        raise BucketSetupError(bucket, "untagged_bucket_contains_data; no_metadata_changed")
    client.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": [marker]})


def validate(namespace: str) -> str:
    if not re.fullmatch(r"mp_acq_[a-z0-9]{3,24}", namespace):
        raise ValueError("Acquisition namespace must be mp_acq_ plus 3-24 lowercase letters/digits")
    return namespace


def raw_bucket(namespace: str) -> str:
    return validate(namespace).replace("_", "-") + "-raw"


def buckets(namespace: str) -> tuple[str, ...]:
    prefix = validate(namespace).replace("_", "-")
    return tuple(prefix + "-" + kind for kind in ("raw", "clean", "quarantine"))


def initialize(engine, client, namespace: str) -> None:
    validate(namespace)
    with engine.begin() as connection:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:n,0))"), {"n": namespace}
        )
        owner = connection.execute(
            text("SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=:n"),
            {"n": namespace},
        ).first()
        if owner is not None and owner[0] != OWNER:
            raise ValueError("Acquisition schema ownership refused")
        if owner is None:
            connection.execute(text(f'CREATE SCHEMA "{namespace}"'))
            connection.execute(text(f"COMMENT ON SCHEMA \"{namespace}\" IS '{OWNER}'"))
        connection.execute(text(f'SET LOCAL search_path TO "{namespace}"'))
        config = Config("alembic.ini")
        config.attributes.update(connection=connection, namespace=namespace)
        command.upgrade(config, "head")
    for bucket in buckets(namespace):
        initialize_bucket(client, bucket, namespace)
    verify(engine, client, namespace)


def verify(engine, client, namespace: str) -> None:
    validate(namespace)
    with engine.connect() as connection:
        owner = connection.scalar(
            text("SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=:n"),
            {"n": namespace},
        )
    if owner != OWNER:
        raise ValueError("Acquisition schema ownership refused; run init first")
    for bucket in buckets(namespace):
        tags = bucket_tags(client, bucket)
        if {"Key": "marketpipe-acquisition", "Value": namespace} not in tags:
            raise BucketSetupError(bucket, "bucket_tag_missing_or_mismatched; run_init_to_check")


@contextmanager
def worker_lock(engine, namespace: str):
    with engine.connect() as connection:
        key = "acquisition-worker:" + validate(namespace)
        acquired = connection.scalar(
            text("SELECT pg_try_advisory_lock(hashtextextended(:n,0))"), {"n": key}
        )
        if not acquired:
            raise ValueError("Another acquisition worker is active; refused")
        try:
            yield
        finally:
            connection.execute(
                text("SELECT pg_advisory_unlock(hashtextextended(:n,0))"), {"n": key}
            )
