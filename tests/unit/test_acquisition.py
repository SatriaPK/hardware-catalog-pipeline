"""Small ingestion suite using the exact runtime adapters with mocked HTTP/storage."""

import json
from pathlib import Path

import httpx
import pytest

from marketplace_pipeline.acquisition import Acquisition
from marketplace_pipeline.acquisition_plan import load_plan
from marketplace_pipeline.adapters.http import HttpSession, Policy
from marketplace_pipeline.adapters.production import Credentials, EbayAdapter, RakutenAdapter
from marketplace_pipeline.storage.acquisition import buckets
from marketplace_pipeline.storage.object_store import ObjectStore
from marketplace_pipeline.storage.parquet import decode
from tests.support import MemoryRepository, MemoryS3


@pytest.mark.parametrize(
    "target,budget,expected",
    [
        (100000, 20, "queries_exhausted"),
        (1, 20, "target_reached"),
        (100000, 1, "request_budget_exhausted"),
    ],
)
def test_collect_parallel_target_shared_budget_and_restart(target, budget, expected):
    from threading import Barrier

    from marketplace_pipeline.runtime import collect

    gate = Barrier(2, timeout=5)
    calls = []
    first_get = set()

    def handler(source):
        normal = handler_for(source, calls)

        def respond(request):
            if budget > 1 and request.method == "GET" and source not in first_get:
                first_get.add(source)
                gate.wait()  # proves both providers can be in HTTP concurrently
            return normal(request)

        return respond

    ebay = memory_acquisition("ebay", handler("ebay"))
    rakuten = Acquisition(
        ebay.repository, ebay.objects, ebay.bucket, adapter("rakuten", handler("rakuten"))
    )
    pipelines = {"ebay": ebay, "rakuten": rakuten}
    events = []
    try:
        result = collect(
            pipelines,
            "collection",
            {s: plan(s) for s in pipelines},
            target,
            budget,
            emit=events.append,
        )
        assert result["stop_reason"] == expected
        assert result["http_attempts_this_invocation"] <= budget
        assert len(calls) == result["http_attempts_this_invocation"]
        if expected == "queries_exhausted":
            assert result["unique_valid_listings"] == 4
            assert first_get == {"ebay", "rakuten"}
            previous = len(calls)
            replay = collect(
                pipelines,
                "collection",
                {s: plan(s) for s in pipelines},
                target,
                budget,
                emit=events.append,
            )
            assert replay["unique_valid_listings"] == 4 and len(calls) == previous
        if expected == "target_reached":
            assert result["unique_valid_listings"] >= target
        assert events[0]["event"] == "started"
    finally:
        for pipeline in pipelines.values():
            pipeline.adapter.session.close()


def test_collect_stops_on_provider_failure_and_keeps_safe_error():
    from marketplace_pipeline.runtime import collect

    ebay = memory_acquisition("ebay", handler_for("ebay", []))
    rakuten = Acquisition(
        ebay.repository,
        ebay.objects,
        ebay.bucket,
        adapter(
            "rakuten",
            lambda _: httpx.Response(403, json={"code": "403", "message": "CLIENT_IP_NOT_ALLOWED"}),
        ),
    )
    pipelines = {"ebay": ebay, "rakuten": rakuten}
    try:
        result = collect(
            pipelines,
            "failure",
            {s: plan(s) for s in pipelines},
            100000,
            10,
            emit=lambda event: None,
        )
        assert result["stop_reason"] == "worker_failed"
        assert result["sources"]["rakuten"]["http_status"] == 403
        assert result["sources"]["rakuten"]["provider_error"]["message"] == "CLIENT_IP_NOT_ALLOWED"
        assert result["http_attempts_this_invocation"] <= 10
    finally:
        for pipeline in pipelines.values():
            pipeline.adapter.session.close()


@pytest.mark.parametrize("has_data", [False, True])
def test_init_repairs_missing_tags_only_for_empty_bucket(has_data):
    from botocore.exceptions import ClientError

    from marketplace_pipeline.storage.acquisition import BucketSetupError, initialize_bucket

    class UntaggedS3(MemoryS3):
        def get_bucket_tagging(self, *, Bucket):
            if not self.buckets[Bucket]:
                raise ClientError({"Error": {"Code": "NoSuchTagSet"}}, "GetBucketTagging")
            return super().get_bucket_tagging(Bucket=Bucket)

        def list_objects_v2(self, **kwargs):
            return {}

        def list_object_versions(self, **kwargs):
            return {"Versions": [{"Key": "existing-private-data"}]} if has_data else {}

    client = UntaggedS3()
    bucket = "mp-acq-test-raw"
    client.create_bucket(Bucket=bucket)
    if has_data:
        with pytest.raises(BucketSetupError, match="untagged_bucket_contains_data"):
            initialize_bucket(client, bucket, "mp_acq_test")
        assert client.buckets[bucket] == []
    else:
        initialize_bucket(client, bucket, "mp_acq_test")
        initialize_bucket(client, bucket, "mp_acq_test")
        assert client.buckets[bucket] == [{"Key": "marketpipe-acquisition", "Value": "mp_acq_test"}]


@pytest.mark.parametrize(
    "code,operation",
    [
        ("NoSuchBucket", "GetBucketTagging"),
        ("AccessDenied", "PutObject"),
        ("PRIVATE_SECRET", "PRIVATE_SECRET"),
    ],
)
def test_batch_reports_safe_storage_error(monkeypatch, capsys, code, operation):
    from botocore.exceptions import ClientError

    from marketplace_pipeline.cli import main

    def fail(args):
        raise ClientError(
            {
                "Error": {"Code": code, "Message": "PRIVATE_SECRET"},
                "ResponseMetadata": {
                    "HTTPStatusCode": 403,
                    "HTTPHeaders": {"secret": "PRIVATE_SECRET"},
                },
            },
            operation,
        )

    monkeypatch.setattr("marketplace_pipeline.runtime.execute", fail)
    assert main(["acquire", "--source", "ebay", "--run-id", "test", "--max-requests", "2"]) == 1
    output = capsys.readouterr().out
    assert "PRIVATE_SECRET" not in output
    result = json.loads(output)
    assert result["operation"] == (
        operation if operation != "PRIVATE_SECRET" else "unrecognized_operation"
    )
    assert result["storage_error_code"] == (
        code if code != "PRIVATE_SECRET" else "unrecognized_code"
    )
    assert result["http_status"] == 403


def adapter(source, handler, retries=0):
    clock = [0.0]

    def sleep(seconds):
        clock[0] += seconds

    session = HttpSession(
        httpx.MockTransport(handler),
        Policy(request_budget=20, max_retries=retries),
        clock=lambda: clock[0],
        sleep=sleep,
        jitter=lambda: 0,
    )
    return (EbayAdapter if source == "ebay" else RakutenAdapter)(
        session, Credentials("TEST_ONLY_ID", "TEST_ONLY_SECRET")
    )


def plan(source):
    return {
        "label": "REAL_API",
        "source": source,
        "marketplace_id": "EBAY_US" if source == "ebay" else "RAKUTEN_JP",
        "retention_days": 7,
        "max_requests_per_run": 20,
        "queries": [
            {
                "query_id": q,
                "query": q,
                "provider": source,
                "marketplace_id": "EBAY_US" if source == "ebay" else "RAKUTEN_JP",
                "category": "gpu",
                "locale": "en-US" if source == "ebay" else "ja-JP",
                "page_size": 2,
                "max_pages": 2,
                "max_results": 4,
            }
            for q in ("query1", "query2")
        ],
    }


def handler_for(source, calls):
    def handler(request):
        calls.append(request)
        if request.method == "POST":
            assert request.url.path == "/identity/v1/oauth2/token"
            assert b"grant_type=client_credentials" in request.content
            return httpx.Response(200, json={"access_token": "TEST_ONLY_TOKEN", "expires_in": 7200})
        if source == "ebay":
            assert request.headers["X-EBAY-C-MARKETPLACE-ID"] == "EBAY_US"
            assert request.headers["Authorization"] == "Bearer TEST_ONLY_TOKEN"
            number = int(request.url.params["offset"]) // 2 + 1
            body = {
                "itemSummaries": [
                    {
                        "itemId": str(number),
                        "title": "TEST_ONLY",
                        "price": {"value": "10", "currency": "USD"},
                    }
                ]
            }
            if number == 1:
                body["next"] = (
                    "https://api.ebay.com/buy/browse/v1/item_summary/search?offset=2&limit=2"
                )
        else:
            assert request.headers["accessKey"] == "TEST_ONLY_SECRET"
            assert "accessKey" not in request.url.params
            assert request.url.params["formatVersion"] == "2"
            number = int(request.url.params["page"])
            body = {
                "items": [{"itemCode": str(number), "itemName": "TEST_ONLY", "itemPrice": 10}],
                "page": number,
                "pageCount": 2,
            }
        return httpx.Response(200, json=body)

    return handler


def memory_acquisition(source, handler):
    client = MemoryS3()
    for bucket in buckets("mp_acq_test"):
        client.create_bucket(Bucket=bucket)
    bucket = buckets("mp_acq_test")[0]
    return Acquisition(
        MemoryRepository(),
        ObjectStore(client),
        bucket,
        adapter(source, handler),
    )


@pytest.mark.parametrize("source", ["ebay", "rakuten"])
def test_pilot_one_data_attempt_then_resume_deduplicates_overlap(source):
    calls = []
    handler = handler_for(source, calls)
    acquisition = memory_acquisition(source, handler)
    acquisition.prepare("test-pilot", plan(source))
    result = acquisition.run("test-pilot", 2 if source == "ebay" else 1, 1)
    assert result["data_attempts"] == result["raw_objects"] == result["accepted"] == 1
    assert result["status"] == "partial"
    acquisition.adapter.session.close()
    acquisition.adapter = adapter(source, handler)
    result = acquisition.run("test-pilot", 10, 10)
    assert result["status"] == "completed"
    assert result["fetched_records"] == 4
    assert result["dataset_unique_accepted_listings"] == result["accepted"] == 2
    assert result["excluded_duplicate"] == 2
    assert sum(r.method == "GET" for r in calls) == 4
    with acquisition.repository.transaction() as tx:
        for manifest in tx.rows("ingest_request"):
            payload = acquisition.objects.read_raw(
                manifest["object_bucket"], manifest["object_key"], manifest["content_sha256"]
            )
            assert len(payload) == manifest["byte_count"]
            assert manifest["rows_reported"] == 1
    previous = len(calls)
    acquisition.adapter.session.close()
    acquisition.adapter = adapter(source, handler)
    assert acquisition.run("test-pilot", 1, 1)["fetched_records"] == 4
    assert len(calls) == previous
    acquisition.adapter.session.close()


@pytest.mark.parametrize(
    "event",
    [
        "after_raw_upload",
        "before_manifest_commit",
        "after_clean_upload",
        "after_quarantine_upload",
        "before_page_commit",
        "after_page_commit",
    ],
)
def test_crash_resume_reuses_raw_without_refetch(event):
    calls = []
    handler = handler_for("rakuten", calls)
    acquisition = memory_acquisition("rakuten", handler)
    specification = plan("rakuten")
    specification["queries"] = [dict(specification["queries"][0], max_pages=1)]
    acquisition.prepare("test-recovery", specification)

    def fail(stage):
        if stage == event:
            raise RuntimeError("TEST_ONLY failure")

    acquisition.hook = fail
    with pytest.raises(RuntimeError):
        acquisition.run("test-recovery", 1, 1)
    acquisition.hook = lambda stage: None
    acquisition.adapter.session.close()
    acquisition.adapter = adapter("rakuten", handler)
    result = acquisition.run("test-recovery", 1, 1)
    assert result["accepted"] == result["raw_objects"] == 1
    assert len(calls) == 1
    acquisition.adapter.session.close()


@pytest.mark.parametrize("status", [401, 403, 429])
def test_pilot_failure_has_no_second_data_attempt(status):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, json={"error": "TEST_ONLY"})

    acquisition = memory_acquisition("rakuten", handler)
    acquisition.prepare("test-stop", plan("rakuten"))
    result = acquisition.run("test-stop", 10, 1)
    assert result["status"] == "failed" and result["raw_objects"] == 0
    assert len(calls) == 1
    acquisition.adapter.session.close()


def test_malformed_success_is_durable_and_not_counted_as_accepted():
    acquisition = memory_acquisition(
        "rakuten", lambda request: httpx.Response(200, content=b"bad-json")
    )
    acquisition.prepare("test-bad", plan("rakuten"))
    result = acquisition.run("test-bad", 1, 1)
    assert result["status"] == "failed"
    assert result["raw_objects"] == result["pending_raw_pages"] == 1
    assert result["accepted"] == 0
    acquisition.adapter.session.close()


def test_plan_has_native_queries_and_provider_bounds():
    for source, market, maximum in (("ebay", "EBAY_US", 200), ("rakuten", "RAKUTEN_JP", 30)):
        specification = load_plan(Path("config/acquisition-plan.yaml"), source, market)
        assert len(specification["queries"]) == 8
        assert all(
            q["page_size"] <= maximum and "?" not in q["query"] for q in specification["queries"]
        )


def test_batch_retries_are_counted_and_still_use_runtime_adapter():
    calls = []
    normal = handler_for("rakuten", calls)
    failed = [False]

    def handler(request):
        if not failed[0]:
            failed[0] = True
            return httpx.Response(429, headers={"Retry-After": "2"})
        return normal(request)

    acquisition = memory_acquisition("rakuten", handler)
    acquisition.adapter = adapter("rakuten", handler, retries=3)
    acquisition.prepare("retry-budget", plan("rakuten"))
    result = acquisition.run("retry-budget", 2, 2)
    assert result["http_attempts"] == result["data_attempts"] == 2
    assert result["accepted"] == 1 and result["status"] == "partial"
    assert acquisition.adapter.session.events[0]["status"] == 429
    acquisition.adapter.session.close()


def test_real_label_is_preserved_in_raw_metadata():
    acquisition = memory_acquisition("rakuten", handler_for("rakuten", []))
    acquisition.objects = ObjectStore(acquisition.objects.client, label="REAL_API")
    acquisition.prepare("label-contract", plan("rakuten"))
    acquisition.run("label-contract", 1, 1)
    metadata = next(iter(acquisition.objects.client.objects.values()))["metadata"]
    assert metadata["label"] == "REAL_API"
    assert metadata["retention-until"] and metadata["content-sha256"]
    assert "TEST_ONLY_SECRET" not in json.dumps(metadata)
    acquisition.adapter.session.close()


@pytest.mark.parametrize("source", ["ebay", "rakuten"])
def test_clean_quarantine_mart_and_conflicting_duplicate(source):
    good = (
        {"itemId": "1", "title": "TEST", "price": {"value": "10", "currency": "USD"}}
        if source == "ebay"
        else {"itemCode": "1", "itemName": "TEST", "itemPrice": 10}
    )
    conflict = {
        **good,
        **(
            {"price": {"value": "20", "currency": "USD"}} if source == "ebay" else {"itemPrice": 20}
        ),
    }
    bad = (
        {"itemId": "bad", "title": "TEST", "price": {"value": "-1", "currency": "BAD"}}
        if source == "ebay"
        else {"itemCode": "bad", "itemName": "TEST", "itemPrice": -1}
    )

    def handler(request):
        if request.method == "POST":
            return httpx.Response(200, json={"access_token": "TEST_TOKEN", "expires_in": 7200})
        rows = [good, good, conflict, bad]
        return httpx.Response(
            200,
            json={"itemSummaries": rows}
            if source == "ebay"
            else {"Items": rows, "page": 1, "pageCount": 1},
        )

    pipeline = memory_acquisition(source, handler)
    specification = plan(source)
    specification["queries"] = [dict(specification["queries"][0], page_size=4, max_pages=1)]
    pipeline.prepare("outputs", specification)
    result = pipeline.run("outputs", 2, 1)
    assert (result["accepted"], result["quarantined"], result["excluded_duplicate"]) == (1, 2, 1)
    with pipeline.repository.transaction() as tx:
        outputs = tx.rows("acquisition_output")
        assert tx.count("dim_listing") == tx.count("fact_price_snapshot") == 1
    for output in outputs:
        body = pipeline.objects.read(
            output["object_bucket"], output["object_key"], output["content_sha256"]
        )
        rows = decode(body) if output["kind"] == "clean" else json.loads(body)
        assert len(rows) == output["row_count"]
        if output["kind"] == "quarantine":
            assert rows[0]["reason_codes"] == ["conflicting_duplicate"]
    # Migrate a completed pre-output run using existing RAW, without another HTTP call.
    pipeline.repository.tables["acquisition_output"] = []
    before = pipeline.adapter.session.attempts
    assert pipeline.run("outputs", 1, 1)["accepted"] == 1
    assert pipeline.adapter.session.attempts == before
    assert pipeline.report()["dataset_unique_accepted_listings"] == 1
    pipeline.adapter.session.close()
