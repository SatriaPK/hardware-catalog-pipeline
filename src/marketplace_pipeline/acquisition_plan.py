"""Validate the executable search plan and provider pagination limits."""

from pathlib import Path

import yaml

from marketplace_pipeline.storage.object_store import safe_segment


def load_plan(path: Path, source: str, marketplace: str) -> dict:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if (
        not isinstance(data, dict)
        or data.get("schema_version") != 1
        or type(data.get("max_requests_per_run")) is not int
        or data["max_requests_per_run"] < 1
    ):
        raise ValueError("Invalid acquisition plan")
    queries = []
    for entry in data["queries"]:
        if entry["provider"] != source:
            continue
        q = dict(entry)
        safe_segment(q["query_id"])
        if q["marketplace_id"] == "CONFIGURED":
            q["marketplace_id"] = marketplace
        if (
            q["marketplace_id"] != marketplace
            or q["category"] not in {"gpu", "ram", "ssd", "keyboard", "mouse", "monitor"}
            or not isinstance(q["query"], str)
            or not q["query"].strip()
            or not isinstance(q["locale"], str)
            or not q["locale"].strip()
        ):
            raise ValueError("Marketplace mismatch or malformed query")
        for field, upper in (
            ("page_size", 200 if source == "ebay" else 30),
            ("max_pages", 100),
            ("max_results", 10000 if source == "ebay" else 3000),
        ):
            if type(q[field]) is not int or not 1 <= q[field] <= upper:
                raise ValueError("Plan exceeds provider bounds")
        if q["max_results"] % q["page_size"]:
            raise ValueError("max_results must be a multiple of page_size; never overshoot a cap")
        queries.append(
            {
                key: q[key]
                for key in (
                    "query_id",
                    "provider",
                    "marketplace_id",
                    "category",
                    "query",
                    "locale",
                    "page_size",
                    "max_pages",
                    "max_results",
                )
            }
        )
    if not queries or len({q["query_id"] for q in queries}) != len(queries):
        raise ValueError("Missing or duplicate query IDs")
    return {
        "schema_version": 1,
        "source": source,
        "marketplace_id": marketplace,
        "label": "REAL_API",
        "queries": queries,
        "max_requests_per_run": data["max_requests_per_run"],
    }
