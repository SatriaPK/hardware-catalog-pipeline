import copy
import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from marketplace_pipeline.acquisition import Acquisition
from marketplace_pipeline.acquisition_plan import load_plan
from marketplace_pipeline.adapters.contracts import SourceAdapter
from marketplace_pipeline.adapters.simulated import SimulatedAdapter, definition
from marketplace_pipeline.cleaning import map_item
from marketplace_pipeline.domain import CanonicalEnvelope
from marketplace_pipeline.mapping import FieldRule, MappingError
from marketplace_pipeline.providers import ProviderRegistry, registry
from marketplace_pipeline.storage.acquisition import buckets
from marketplace_pipeline.storage.object_store import IntegrityConflict, ObjectStore
from marketplace_pipeline.storage.parquet import decode, encode
from tests.support import MemoryRepository, MemoryS3


def context(source="simulated", **extra):
    return dict(
        source=source,
        marketplace_id={"ebay": "EBAY_US", "rakuten": "RAKUTEN_JP", "simulated": "SIMULATION"}[
            source
        ],
        category="gpu",
        run_id="test",
        request_id="request",
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
        **extra,
    )


def register():
    if "simulated" not in registry.names():
        registry.register(definition())


def document():
    return json.loads(Path("tests/fixtures/simulated/catalog.json").read_text(encoding="utf-8"))


def simulation():
    register()
    client = MemoryS3()
    for bucket in buckets("mp_acq_simulation"):
        client.create_bucket(Bucket=bucket)
    return Acquisition(
        MemoryRepository(),
        ObjectStore(client),
        buckets("mp_acq_simulation")[0],
        SimulatedAdapter([document()]),
    )


def specification():
    register()
    return load_plan(Path("config/simulated-plan.yaml"), "simulated", "SIMULATION")


def test_registry_contract_and_fail_closed():
    local = ProviderRegistry()
    local.register(definition())
    assert local.names() == ("simulated",)
    assert local.names(live_only=True) == ()
    with pytest.raises(ValueError, match="already registered"):
        local.register(definition())
    with pytest.raises(ValueError, match="Unregistered"):
        local.get("not-a-provider")
    with pytest.raises(ValueError, match="Unsupported mapping"):
        local.get("simulated").mapping("unknown")
    pipeline = simulation()
    try:
        assert isinstance(pipeline.adapter, SourceAdapter)
        for source in ("ebay", "rakuten"):
            assert registry.get(source).capabilities.authentication != "none"
    finally:
        pipeline.adapter.session.close()


@pytest.mark.parametrize(
    "source,item,currency",
    [
        (
            "ebay",
            {"itemId": "1", "title": " GPU ", "price": {"value": "1.20", "currency": "USD"}},
            "USD",
        ),
        ("rakuten", {"itemCode": "1", "itemName": "GPU", "itemPrice": 200}, "JPY"),
    ],
)
def test_builtin_mapping_and_legacy_hash(source, item, currency):
    modern, reasons, _ = map_item(item, context(source))
    legacy, _, _ = map_item(
        item, context(source, mapping_version="legacy-1", canonical_schema_version=1)
    )
    assert not reasons and modern.currency_code == currency
    assert modern.record_hash == legacy.record_hash
    assert modern.attributes_json == item and legacy.attributes_json == {}
    assert decode(encode([modern])) == [modern]
    assert decode(encode([legacy], version=1)) == [legacy]
    with pytest.raises(ValueError, match="version differs"):
        encode([modern], version=1)
    assert (
        CanonicalEnvelope.model_validate_json(modern.envelope().model_dump_json())
        == modern.envelope()
    )


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"offer": {"cost": True, "currency": "USD"}}, "mapping.invalid_type.amount"),
        ({"offer": {"cost": "NaN", "currency": "USD"}}, "mapping.invalid_type.amount"),
        ({"offer": {"cost": "1.12345", "currency": "USD"}}, "canonical_schema_violation"),
        ({"offer": {"cost": "1", "currency": "ZZZ"}}, "unsupported_or_invalid_currency"),
        ({"description": {"label": "one", "name": "different"}}, "mapping.ambiguous_alias.title"),
        ({"manufacturer": {"name": 123}}, "mapping.invalid_type.brand"),
    ],
)
def test_incompatible_values_have_reasons(change, reason):
    register()
    item = {**document()["catalog"]["offers"][0], **change}
    row, reasons, _ = map_item(item, context())
    assert row is None and reason in reasons


def test_optional_fields_aliases_nested_evolution_and_provenance():
    register()
    item = document()["catalog"]["offers"][0]
    row, _, _ = map_item(item, context())
    evolved = copy.deepcopy(item)
    evolved["description"] = {"name": evolved["description"]["label"]}
    evolved["offer"] = {"cost": "12.50", "currency": "USD"}
    evolved["new_attribute"] = ["preserved", {"flag": True}]
    second, reasons, _ = map_item(evolved, context())
    assert not reasons and second.record_hash == row.record_hash
    assert second.attributes_json["new_attribute"] == evolved["new_attribute"]
    assert second.brand is None and second.title_normalized == "GPU é"
    with pytest.raises(ValidationError):
        CanonicalEnvelope.model_validate({**second.envelope().model_dump(), "entity_kind": "sale"})
    with pytest.raises(ValidationError):
        CanonicalEnvelope.model_validate({**second.envelope().model_dump(), "raw_request_id": ""})


def test_explicit_default_and_datetime_conversion():
    assert FieldRule("x", ("nested.x",), default="declared").read({}) == "declared"
    rule = FieldRule("stamp", ("date",), "datetime", True)
    assert rule.read({"date": "2026-01-01T07:00:00+07:00"}) == datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(MappingError, match="invalid_type"):
        rule.read({"date": "2026-01-01"})


def test_third_source_all_layers_replay_and_new_observation():
    pipeline = simulation()
    try:
        pipeline.prepare("first", specification())
        result = pipeline.run("first", 10, 10)
        assert (result["accepted"], result["quarantined"], result["excluded_duplicate"]) == (
            2,
            2,
            1,
        )
        assert result["label"] == "TEST_ONLY"
        raw = result["raw_locations"][0]
        body = pipeline.objects.read_raw(
            raw["object_bucket"], raw["object_key"], raw["content_sha256"]
        )
        assert json.loads(body) == document()
        outputs = {
            o["kind"]: pipeline.objects.read(
                o["object_bucket"], o["object_key"], o["content_sha256"]
            )
            for o in result["output_locations"]
        }
        clean, quarantine = decode(outputs["clean"]), json.loads(outputs["quarantine"])
        assert len(clean) == len(quarantine) == 2
        assert clean[0].attributes_json["warranty"]["months"] == 24
        assert all(row.raw_request_id == quarantine[0]["raw_request_id"] for row in clean)
        before = pipeline.adapter.session.attempts
        assert pipeline.run("first", 10, 10)["accepted"] == 2
        assert pipeline.adapter.session.attempts == before
        assert pipeline.repository.count("fact_price_snapshot") == 2
        pipeline.prepare("second", specification())
        pipeline.run("second", 10, 10)
        assert pipeline.repository.count("fact_price_snapshot") == 4
        assert pipeline.repository.count("dim_listing") == 2
    finally:
        pipeline.adapter.session.close()


@pytest.mark.parametrize(
    "stage",
    [
        "after_raw_upload",
        "before_manifest_commit",
        "after_clean_upload",
        "after_quarantine_upload",
        "before_page_commit",
        "after_page_commit",
    ],
)
def test_third_source_recovery(stage):
    pipeline = simulation()
    try:
        pipeline.prepare("recover", specification())

        def fail(name):
            if name == stage:
                raise RuntimeError("SIMULATED INTERRUPTION")

        pipeline.hook = fail
        with pytest.raises(RuntimeError):
            pipeline.run("recover", 10, 10)
        pipeline.hook = lambda _: None
        result = pipeline.run("recover", 10, 10)
        assert result["accepted"] == 2 and result["quarantined"] == 2
        assert pipeline.adapter.session.attempts == 1
        assert pipeline.repository.count("fact_price_snapshot") == 2
    finally:
        pipeline.adapter.session.close()


def test_raw_integrity_and_schema_version_conflicts():
    pipeline = simulation()
    try:
        pipeline.prepare("integrity", specification())
        pipeline.adapter.endpoint_version = "simulation-2"
        with pytest.raises(IntegrityConflict, match="Source schema"):
            pipeline.run("integrity", 10, 10)
        pipeline.adapter.endpoint_version = "simulation-1"
        result = pipeline.run("integrity", 10, 10)
        raw = result["raw_locations"][0]
        obj = pipeline.objects.client.objects[raw["object_bucket"], raw["object_key"]]
        obj["body"] = gzip.compress(b"tampered")
        with pytest.raises(IntegrityConflict):
            pipeline.objects.read_raw(
                raw["object_bucket"], raw["object_key"], raw["content_sha256"]
            )
    finally:
        pipeline.adapter.session.close()


def test_new_source_schema_version_and_mapping_pin():
    pipeline = simulation()
    try:
        evolved = document()
        evolved["version"] = "simulation-2"
        pipeline.adapter.session.close()
        pipeline.adapter = SimulatedAdapter([evolved], version="simulation-2")
        with pytest.raises(ValueError, match="does not support"):
            pipeline.prepare("old-map", dict(specification(), mapping_version="1"))
        pipeline.prepare("evolved", dict(specification(), mapping_version="2"))
        assert pipeline.run("evolved", 10, 10)["accepted"] == 2
        stored = pipeline.state("evolved")[1]
        assert stored["source_schema_version"] == "simulation-2"
        assert stored["mapping_version"] == "2"
    finally:
        pipeline.adapter.session.close()


def test_three_source_collection_and_namespace_noninterference():
    from marketplace_pipeline.runtime import collect
    from tests.unit.test_acquisition import adapter, handler_for, plan

    simulated = simulation()
    untouched = simulation()
    untouched.prepare("private-run", specification())
    untouched.run("private-run", 10, 10)
    previous = copy.deepcopy(untouched.repository.tables)
    previous_objects = copy.deepcopy(untouched.objects.client.objects)
    pipelines = {"simulated": simulated}
    plans = {"simulated": specification()}
    for source in ("ebay", "rakuten"):
        pipelines[source] = Acquisition(
            simulated.repository,
            simulated.objects,
            simulated.bucket,
            adapter(source, handler_for(source, [])),
        )
        plans[source] = plan(source)
    try:
        result = collect(pipelines, "combined", plans, 100, 30, emit=lambda _: None)
        assert result["unique_valid_listings"] == 6
        assert result["stop_reason"] == "queries_exhausted"
        assert simulated.repository.count("fact_price_snapshot") == 6
        assert {r["provider"] for r in simulated.repository.rows("dim_listing")} == set(pipelines)
        collect(pipelines, "combined", plans, 100, 30, emit=lambda _: None)
        assert simulated.repository.count("fact_price_snapshot") == 6
        assert untouched.repository.tables == previous
        assert untouched.objects.client.objects == previous_objects
    finally:
        for pipeline in [*pipelines.values(), untouched]:
            pipeline.adapter.session.close()


def test_legacy_unpinned_run_and_orphan_output_recovery():
    from marketplace_pipeline.cleaning import hash_fields
    from tests.unit.test_acquisition import handler_for, memory_acquisition, plan

    pipeline = memory_acquisition("ebay", handler_for("ebay", []))
    try:
        pipeline.prepare("legacy", plan("ebay"))
        # Reconstruct the exact old plan shape, as present before this refactor.
        old_plan = dict(plan("ebay"), raw_bucket=pipeline.bucket)
        with pipeline.repository.transaction() as tx:
            tx.change(
                "run_plan",
                {"run_id": "legacy"},
                {"plan": old_plan, "plan_hash": hash_fields(old_plan)},
            )
            tx.change(
                "pipeline_run", {"run_id": "legacy"}, {"query_plan_hash": hash_fields(old_plan)}
            )

        def fail(stage):
            if stage == "after_clean_upload":
                raise RuntimeError("legacy interruption")

        pipeline.hook = fail
        with pytest.raises(RuntimeError):
            pipeline.run("legacy", 10, 10)
        pipeline.hook = lambda _: None
        result = pipeline.run("legacy", 10, 10)
        assert result["accepted"] == 2
        for output in result["output_locations"]:
            if output["kind"] == "clean":
                rows = decode(
                    pipeline.objects.read(
                        output["object_bucket"], output["object_key"], output["content_sha256"]
                    )
                )
                assert all(r.schema_version == 1 and r.mapping_version == "legacy-1" for r in rows)
    finally:
        pipeline.adapter.session.close()


def test_invalid_extension_is_quarantined_and_live_label_refused():
    register()
    item = dict(document()["catalog"]["offers"][0], unexpected=float("nan"))
    row, reasons, _ = map_item(item, context())
    assert row is None and reasons == ["canonical_schema_violation"]
    pipeline = simulation()
    try:
        pipeline.objects.label = "REAL_API"
        with pytest.raises(ValueError, match="TEST_ONLY"):
            pipeline.prepare("not-real", specification())
    finally:
        pipeline.adapter.session.close()


def test_core_has_no_builtin_provider_dispatch():
    import ast

    root = Path("src/marketplace_pipeline")
    for name in ("acquisition.py", "acquisition_plan.py", "cleaning.py", "runtime.py", "cli.py"):
        tree = ast.parse((root / name).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert node.value not in {"ebay", "rakuten", "simulated"}, name


@pytest.mark.parametrize(
    "source,item",
    [
        ("ebay", {"itemId": "x", "title": "GPU", "price": {"value": "10", "currency": "USD"}}),
        ("rakuten", {"itemCode": "x", "itemName": "GPU", "itemPrice": 10}),
    ],
)
def test_builtin_optional_invalid_types_are_not_silently_coerced(source, item):
    row, reasons, _ = map_item(dict(item, brand=123), context(source))
    assert row is None and "mapping.invalid_type.brand" in reasons
    legacy, reasons, _ = map_item(
        dict(item, brand=123),
        context(source, mapping_version="legacy-1", canonical_schema_version=1),
    )
    assert legacy is not None and legacy.brand is None and not reasons


def test_mapping_rules_are_independent_of_hardware_category_vocabulary():
    from dataclasses import replace

    register()
    original = registry.get("simulated")
    custom = replace(
        original, source_id="category_test", policy=replace(original.policy, categories=("books",))
    )
    if custom.source_id not in registry.names():
        registry.register(custom)
    item = document()["catalog"]["offers"][0]
    ctx = dict(context(), source=custom.source_id, category="books")
    row, reasons, _ = map_item(item, ctx)
    assert not reasons and row.category_canonical == "books"


def test_forward_migration_changes_only_source_constraint():
    import importlib.util
    from io import StringIO

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    spec = importlib.util.spec_from_file_location(
        "migration_0007", "migrations/versions/0007_extensible_sources.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    output = StringIO()
    ctx = MigrationContext.configure(
        dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output}
    )
    with Operations.context(ctx):
        module.upgrade()
    sql = output.getvalue()
    assert "DROP CONSTRAINT pipeline_run_source_check" in sql
    assert "VALIDATE CONSTRAINT" in sql
    assert all(token not in sql for token in ("DROP TABLE", "DELETE", "TRUNCATE"))
