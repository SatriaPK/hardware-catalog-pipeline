"""Opt-in storage integration; mocked provider HTTP, isolated retained resources."""

import os
from uuid import uuid4

import pytest

from marketplace_pipeline.acquisition import Acquisition
from marketplace_pipeline.settings import Settings
from marketplace_pipeline.storage.acquisition import initialize, raw_bucket
from marketplace_pipeline.storage.connections import minio_client, postgres_engine
from marketplace_pipeline.storage.object_store import ObjectStore
from marketplace_pipeline.storage.parquet import decode
from marketplace_pipeline.storage.repository import PostgresRepository
from tests.unit.test_acquisition import adapter, handler_for, plan

pytestmark = pytest.mark.storage


@pytest.mark.parametrize("source", ["ebay", "rakuten"])
def test_storage_pipeline_and_resume(source):
    settings = Settings.from_environ(os.environ)
    namespace = "mp_acq_" + uuid4().hex[:16]
    engine = postgres_engine(settings)
    client = minio_client(settings)
    http = adapter(source, handler_for(source, []))
    try:
        initialize(engine, client, namespace)
        initialize(engine, client, namespace)
        repo = PostgresRepository(engine, namespace)
        objects = ObjectStore(client)
        pipeline = Acquisition(repo, objects, raw_bucket(namespace), http)
        pipeline.prepare("test-run", plan(source))
        result = pipeline.run("test-run", 10, 10)
        assert result["accepted"] == result["excluded_duplicate"] == 2
        assert result["clean_objects"] == result["quarantine_objects"] == 4
        with repo.transaction() as tx:
            manifests = tx.rows("acquisition_output", run_id="test-run")
            assert tx.count("dim_listing") == 2
        clean = [
            row
            for m in manifests
            if m["kind"] == "clean"
            for row in decode(
                objects.read(m["object_bucket"], m["object_key"], m["content_sha256"])
            )
        ]
        assert len(clean) == 2
        attempts = http.session.attempts
        assert pipeline.run("test-run", 1, 1)["accepted"] == 2
        assert http.session.attempts == attempts
        print("Retained isolated test namespace:", namespace)
    finally:
        http.session.close()
        engine.dispose()
    # No DROP, bucket delete or automatic cleanup.
