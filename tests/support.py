"""In-memory test doubles only; production runtime always uses PostgreSQL/MinIO."""

import copy
import io
from contextlib import contextmanager
from threading import RLock

from botocore.exceptions import ClientError

from marketplace_pipeline.storage.object_store import IntegrityConflict
from marketplace_pipeline.storage.repository import TABLES


class MemoryRepository:
    def __init__(self):
        self.tables = {name: [] for name in TABLES}
        self.lock = RLock()

    @contextmanager
    def transaction(self):
        with self.lock:
            backup = copy.deepcopy(self.tables)
            try:
                yield self
            except Exception:
                self.tables = backup
                raise

    def rows(self, table, **where):
        return copy.deepcopy(
            [row for row in self.tables[table] if all(row.get(k) == v for k, v in where.items())]
        )

    def count(self, table):
        return len(self.tables[table])

    def put(self, table, key, values):
        values = {**key, **values}
        existing = self.rows(table, **key)
        if existing:
            if any(existing[0].get(k) != v for k, v in values.items()):
                raise IntegrityConflict(f"Immutable {table} row conflict")
            return False
        self.tables[table].append(copy.deepcopy(values))
        return True

    def change(self, table, key, values):
        for row in self.tables[table]:
            if all(row.get(k) == v for k, v in key.items()):
                row.update(copy.deepcopy(values))


def error(code):
    return ClientError({"Error": {"Code": code}}, "TEST_ONLY")


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.buckets = {}
        self.writes = 0

    def head_bucket(self, *, Bucket):
        if Bucket not in self.buckets:
            raise error("404")

    def create_bucket(self, *, Bucket):
        if Bucket in self.buckets:
            raise error("BucketAlreadyOwnedByYou")
        self.buckets[Bucket] = []

    def put_bucket_tagging(self, *, Bucket, Tagging):
        self.buckets[Bucket] = Tagging["TagSet"]

    def get_bucket_tagging(self, *, Bucket):
        return {"TagSet": self.buckets[Bucket]}

    def put_object(self, *, Bucket, Key, Body, ContentType, Metadata, IfNoneMatch):
        self.head_bucket(Bucket=Bucket)
        assert IfNoneMatch == "*"
        if (Bucket, Key) in self.objects:
            raise error("PreconditionFailed")
        self.objects[Bucket, Key] = {"body": Body, "metadata": Metadata}
        self.writes += 1

    def head_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise error("NoSuchKey")
        record = self.objects[Bucket, Key]
        return {"Metadata": record["metadata"], "ContentLength": len(record["body"])}

    def get_object(self, *, Bucket, Key):
        self.head_object(Bucket=Bucket, Key=Key)
        return {"Body": io.BytesIO(self.objects[Bucket, Key]["body"])}
