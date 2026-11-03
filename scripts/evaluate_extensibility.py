"""Reproduce simulation-only quality metrics. Uses memory storage, never credentials."""

import json
import socket
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from marketplace_pipeline.cleaning import map_item  # noqa: E402
from marketplace_pipeline.mapping import MISSING, path_value  # noqa: E402
from marketplace_pipeline.providers import registry  # noqa: E402
from marketplace_pipeline.storage.parquet import decode  # noqa: E402
from tests.unit.test_extensibility import context, document, simulation, specification  # noqa: E402


def evaluate():
    pipeline = simulation()
    try:
        pipeline.prepare("evaluation", specification())
        result = pipeline.run("evaluation", 10, 10)
        items = document()["catalog"]["offers"]
        spec = registry.get("simulated").mapping()
        conformant = sum(map_item(item, context())[0] is not None for item in items)
        # Denominator: present input values in the explicitly declared mapping scope.
        present, converted = 0, 0
        for item in items:
            values, _ = spec.apply(item)
            for rule in spec.fields:
                if any(path_value(item, path) is not MISSING for path in rule.paths):
                    present += 1
                    converted += values.get(rule.target) is not None
        clean = []
        for output in result["output_locations"]:
            if output["kind"] == "clean":
                clean.extend(
                    decode(
                        pipeline.objects.read(
                            output["object_bucket"], output["object_key"], output["content_sha256"]
                        )
                    )
                )
        raw_ids = {r["request_id"] for r in pipeline.repository.rows("ingest_request")}
        traceable = sum(row.raw_request_id in raw_ids for row in clean)
        return dict(
            label="OFFLINE_SIMULATION",
            live_api_records_acquired=0,
            occurrences=len(items),
            schema_conformance=dict(numerator=conformant, denominator=len(items)),
            mapping_coverage=dict(
                numerator=converted,
                denominator=present,
                scope="present declared field values; successful conversions, not complete records",
            ),
            outcomes={k: result[k] for k in ("accepted", "quarantined", "excluded_duplicate")},
            clean_provenance=dict(numerator=traceable, denominator=len(clean)),
            core_files_changed_for_third_source=0,
            provider_implementation_modules_added=1,
            fixture_files_added=1,
            configuration_files_added=1,
        )
    finally:
        pipeline.adapter.session.close()


if __name__ == "__main__":

    def denied(*args, **kwargs):
        raise RuntimeError("Network forbidden in offline evaluation")

    socket.socket.connect = denied
    socket.socket.connect_ex = denied
    socket.create_connection = denied
    result = json.dumps(evaluate(), indent=2)
    if len(sys.argv) > 1:
        Path(sys.argv[1]).write_text(result + "\n", encoding="utf-8")
    else:
        print(result)
