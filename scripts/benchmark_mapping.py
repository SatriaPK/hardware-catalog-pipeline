"""Fixed offline mapping workload; no API or storage access."""

import json
import platform
import statistics
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from marketplace_pipeline.cleaning import map_item  # noqa: E402


def benchmark():
    stamp = datetime(2026, 1, 1, tzinfo=UTC)
    context = dict(
        source="ebay",
        marketplace_id="EBAY_US",
        category="gpu",
        run_id="benchmark",
        request_id="request",
        observed_at=stamp,
        ingested_at=stamp,
    )
    item = dict(itemId="example", title=" GPU e\u0301 ", price=dict(value="123.45", currency="USD"))
    samples = []
    for _ in range(5):
        started = time.perf_counter()
        for _ in range(10000):
            row, reasons, _ = map_item(item, context)
            assert row is not None and not reasons
        samples.append(time.perf_counter() - started)
    return dict(
        label="OFFLINE_SIMULATION",
        python=platform.python_version(),
        records_per_sample=10000,
        samples_seconds=samples,
        median_seconds=statistics.median(samples),
        records_per_second=10000 / statistics.median(samples),
        network_seconds=0,
    )


if __name__ == "__main__":
    result = json.dumps(benchmark(), indent=2)
    if len(sys.argv) > 1:
        Path(sys.argv[1]).write_text(result + "\n", encoding="utf-8")
    else:
        print(result)
