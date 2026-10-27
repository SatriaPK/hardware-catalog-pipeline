"""Durable real API acquisition. No fixture payloads, no hidden network on replay."""

import gzip
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from botocore.exceptions import ClientError

from marketplace_pipeline.adapters.http import ContractError
from marketplace_pipeline.cleaning import hash_fields, map_item
from marketplace_pipeline.storage.object_store import IntegrityConflict, digest, safe_segment
from marketplace_pipeline.storage.parquet import encode


class Acquisition:
    def __init__(self, repository, objects, bucket, adapter, *, hook=lambda name: None):
        self.repository, self.objects, self.bucket = repository, objects, bucket
        self.adapter, self.hook = adapter, hook
        self.clean_bucket = bucket.removesuffix("-raw") + "-clean"
        self.quarantine_bucket = bucket.removesuffix("-raw") + "-quarantine"

    def prepare(self, run_id: str, plan: dict) -> None:
        safe_segment(run_id)
        plan = {**plan, "raw_bucket": self.bucket}
        with self.repository.transaction() as tx:
            old = tx.rows("pipeline_run", run_id=run_id)
            if old:
                raise ValueError("Run already exists; use resume")
            tx.put(
                "pipeline_run",
                {"run_id": run_id},
                {
                    "source": plan["source"],
                    "marketplace_id": plan["marketplace_id"],
                    "mode": "live",
                    "started_at": datetime.now(UTC),
                    "status": "pending",
                    "query_plan_hash": hash_fields(plan),
                },
            )
            tx.put("run_plan", {"run_id": run_id}, {"plan": plan, "plan_hash": hash_fields(plan)})

    def state(self, run_id):
        safe_segment(run_id)
        with self.repository.transaction() as tx:
            runs, plans = tx.rows("pipeline_run", run_id=run_id), tx.rows("run_plan", run_id=run_id)
        if len(runs) != 1 or len(plans) != 1 or runs[0]["mode"] != "live":
            raise ValueError("Unknown acquisition run")
        plan = plans[0]["plan"]
        if (
            plans[0]["plan_hash"] != hash_fields(plan)
            or runs[0]["query_plan_hash"] != hash_fields(plan)
            or plan["raw_bucket"] != self.bucket
            or plan["source"] != self.adapter.source_id
        ):
            raise IntegrityConflict("Acquisition plan mismatch")
        return runs[0], plan

    def run(
        self,
        run_id,
        max_requests: int,
        max_data_requests: int,
        *,
        stop=lambda: None,
        before_request=lambda: None,
        progress=None,
    ) -> dict:
        run, plan = self.state(run_id)
        invocation = uuid4().hex
        with self.repository.transaction() as tx:
            tx.put(
                "acquisition_invocation",
                {"invocation_id": invocation},
                {
                    "run_id": run_id,
                    "started_at": datetime.now(UTC),
                    "max_requests": max_requests,
                    "max_data_requests": max_data_requests,
                    "http_attempts": 0,
                    "data_attempts": 0,
                },
            )

        def reserve(method, url):
            reason = stop()
            if reason:
                raise ContractError(reason)
            with self.repository.transaction() as tx:
                record = tx.rows("acquisition_invocation", invocation_id=invocation)[0]
                total = sum(
                    r["http_attempts"] for r in tx.rows("acquisition_invocation", run_id=run_id)
                )
                if (
                    record["http_attempts"] >= max_requests
                    or total >= plan["max_requests_per_run"]
                    or (method == "GET" and record["data_attempts"] >= max_data_requests)
                ):
                    raise ContractError("request_budget_exhausted")
                before_request()
                tx.change(
                    "acquisition_invocation",
                    {"invocation_id": invocation},
                    {
                        "http_attempts": record["http_attempts"] + 1,
                        "data_attempts": record["data_attempts"] + int(method == "GET"),
                    },
                )

        self.adapter.session.before_attempt = reserve
        status, error_code = "completed", None
        try:
            for query in plan["queries"]:
                page = 1
                while (
                    page <= query["max_pages"]
                    and (page - 1) * query["page_size"] < query["max_results"]
                ):
                    reason = stop()
                    if reason:
                        raise ContractError(reason)
                    request_id = digest(f"{run_id}|{query['query_id']}|{page}".encode())
                    with self.repository.transaction() as tx:
                        previous = tx.rows("acquisition_page", request_id=request_id)
                        if previous and previous[0]["status"] == "completed":
                            # Older completed runs may predate CLEAN/QUARANTINE outputs.
                            if not tx.rows("acquisition_output", request_id=request_id):
                                old_key = f"{plan['source']}/{run_id}/{request_id}.json.gz"
                                # A completed page must never be re-fetched if its RAW is lost.
                                self.objects.client.head_object(Bucket=self.bucket, Key=old_key)
                                old_body, old_observed, _ = self.raw(
                                    query, page, request_id, old_key, plan.get("retention_days")
                                )
                                old_items, _ = self.adapter.parse(old_body, query, page)
                                self.write_outputs(
                                    tx, run_id, query, request_id, old_items, old_observed
                                )
                            next_page = previous[0]["next_page"]
                            if next_page is None:
                                break
                            page = next_page
                            continue
                        if not previous:
                            tx.put(
                                "acquisition_page",
                                {"request_id": request_id},
                                {
                                    "run_id": run_id,
                                    "query_id": query["query_id"],
                                    "page_number": page,
                                    "status": "prepared",
                                    "next_page": None,
                                    "rows_fingerprint": None,
                                },
                            )
                    key = f"{plan['source']}/{run_id}/{request_id}.json.gz"
                    body, observed, _ = self.raw(
                        query, page, request_id, key, plan.get("retention_days")
                    )
                    self.hook("after_raw_upload")
                    with self.repository.transaction() as tx:
                        tx.put(
                            "ingest_request",
                            {"request_id": request_id},
                            {
                                "run_id": run_id,
                                "source": plan["source"],
                                "marketplace_id": plan["marketplace_id"],
                                "query_id": query["query_id"],
                                "page_number": page,
                                "endpoint_version": self.adapter.endpoint_version,
                                "safe_query_hash": hash_fields(query),
                                "observed_at": observed,
                                "ingested_at": observed,
                                "object_bucket": self.bucket,
                                "object_key": key,
                                "content_sha256": digest(body),
                                "byte_count": len(body),
                                "http_status": 200,
                                "rows_reported": None,
                                "status": "durable",
                                "schema_version": 1,
                            },
                        )
                        tx.change(
                            "acquisition_page", {"request_id": request_id}, {"status": "durable"}
                        )
                        self.hook("before_manifest_commit")
                    items, more = self.adapter.parse(body, query, page)
                    if len(items) > query["page_size"]:
                        raise ContractError("page_size_violation")
                    more = (
                        more
                        and page < query["max_pages"]
                        and page * query["page_size"] < query["max_results"]
                    )
                    self.commit_page(run_id, query, page, request_id, items, more, observed)
                    self.hook("after_page_commit")
                    if progress is not None:
                        with self.repository.transaction() as tx:
                            occurrences = tx.rows("acquisition_occurrence", request_id=request_id)
                        progress(
                            {
                                name: sum(o["outcome"] == name for o in occurrences)
                                for name in ("accepted", "quarantined", "excluded_duplicate")
                            }
                        )
                    if not more:
                        break
                    page += 1
        except ContractError as error:
            status = (
                "partial"
                if str(error)
                in {"request_budget_exhausted", "target_reached", "collection_stopped"}
                else "failed"
            )
            error_code = str(error)
        except Exception:
            with self.repository.transaction() as tx:
                tx.change(
                    "pipeline_run",
                    {"run_id": run_id},
                    {"status": "failed", "error_summary": "acquisition_interrupted"},
                )
            raise
        with self.repository.transaction() as tx:
            tx.change(
                "pipeline_run", {"run_id": run_id}, {"status": status, "error_summary": error_code}
            )
        return {**self.report(run_id), "stop_reason": error_code}

    def raw(self, query, page, request_id, key, retention_days):
        try:
            head = self.objects.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"404", "NoSuchKey", "NotFound"}:
                raise
            head = None
        if head is None:
            body = self.adapter.fetch(query, page)
            observed = datetime.now(UTC)
            expires = observed + timedelta(days=retention_days) if retention_days else None
            self.objects.put(
                self.bucket,
                key,
                gzip.compress(body, mtime=0),
                "application/gzip",
                {
                    "content-sha256": digest(body),
                    "request-id": request_id,
                    "observed-at": observed.isoformat(),
                    **({"retention-until": expires.isoformat()} if expires else {}),
                },
            )
        else:
            metadata = head["Metadata"]
            if (
                metadata.get("request-id") != request_id
                or metadata.get("label") != self.objects.label
            ):
                raise IntegrityConflict("Acquisition RAW ownership conflict")
            observed = datetime.fromisoformat(metadata["observed-at"])
            expires = (
                datetime.fromisoformat(metadata["retention-until"])
                if metadata.get("retention-until")
                else None
            )
            if observed.tzinfo is None or (
                expires is not None and (expires.tzinfo is None or expires <= datetime.now(UTC))
            ):
                raise ValueError("RAW retention expired; replay refused")
            body = self.objects.read_raw(self.bucket, key, metadata["content-sha256"])
        return body, observed, expires

    def commit_page(self, run_id, query, page, request_id, items, more, observed):
        fingerprint = hash_fields({"items": items})
        with self.repository.transaction() as tx:
            if items and any(
                r["rows_fingerprint"] == fingerprint and r["query_id"] == query["query_id"]
                for r in tx.rows("acquisition_page", run_id=run_id)
                if r["request_id"] != request_id
            ):
                raise ContractError("repeated_page")
            tx.put(
                "dim_marketplace",
                {"marketplace_id": query["marketplace_id"]},
                {"provider": query["provider"], "region": "API_PRODUCTION"},
            )
            for index, item in enumerate(items):
                row, reasons, _ = map_item(
                    item,
                    {
                        "source": query["provider"],
                        "marketplace_id": query["marketplace_id"],
                        "category": query["category"],
                        "run_id": run_id,
                        "request_id": request_id,
                        "observed_at": observed,
                        "ingested_at": observed,
                    },
                )
                identity = item.get("itemId" if query["provider"] == "ebay" else "itemCode")
                key = (
                    digest(f"{query['provider']}|{query['marketplace_id']}|{identity}".encode())
                    if isinstance(identity, str) and identity.strip()
                    else None
                )
                outcome = "quarantined"
                if row is not None:
                    snapshot_key = digest(f"{run_id}|{key}".encode())
                    existing = tx.rows("fact_price_snapshot", snapshot_key=snapshot_key)
                    if existing:
                        if existing[0]["record_hash"] == row.record_hash:
                            outcome, reasons = "excluded_duplicate", ["duplicate_same_listing"]
                        else:
                            reasons = ["conflicting_duplicate"]
                    else:
                        old_listing = tx.rows("dim_listing", listing_key=key)
                        if not old_listing:
                            tx.put(
                                "dim_listing",
                                {"listing_key": key},
                                {
                                    "provider": row.source,
                                    "marketplace_id": row.marketplace_id,
                                    "source_listing_id": row.listing_id,
                                    "product_key": None,
                                    "title_original": row.title_original,
                                    "category_canonical": row.category_canonical,
                                    "first_seen_at": observed,
                                    "last_seen_at": observed,
                                },
                            )
                        elif observed > old_listing[0]["last_seen_at"]:
                            tx.change(
                                "dim_listing",
                                {"listing_key": key},
                                {
                                    "last_seen_at": observed,
                                    "title_original": row.title_original,
                                    "category_canonical": row.category_canonical,
                                },
                            )
                        prior = (
                            tx.rows("fact_price_snapshot", listing_key=key) if old_listing else []
                        )
                        latest = (
                            max(prior, key=lambda s: (s["observed_at"], s["run_id"]))
                            if prior
                            else None
                        )
                        change_kind = (
                            "new"
                            if latest is None
                            else "unchanged"
                            if latest["record_hash"] == row.record_hash
                            else "changed"
                        )
                        tx.put(
                            "fact_price_snapshot",
                            {"snapshot_key": snapshot_key},
                            {
                                "run_id": run_id,
                                "listing_key": key,
                                "raw_request_id": request_id,
                                "observed_at": observed,
                                "price_amount": row.price_amount,
                                "currency_code": row.currency_code,
                                "record_hash": row.record_hash,
                                "change_kind": change_kind,
                            },
                        )
                        outcome = "accepted"
                tx.put(
                    "acquisition_occurrence",
                    {"request_id": request_id, "row_index": index},
                    {
                        "run_id": run_id,
                        "listing_key": key,
                        "outcome": outcome,
                        "reason_codes": reasons,
                    },
                )
            self.write_outputs(tx, run_id, query, request_id, items, observed)
            tx.change(
                "acquisition_page",
                {"request_id": request_id},
                {
                    "status": "completed",
                    "next_page": page + 1 if more else None,
                    "rows_fingerprint": fingerprint,
                },
            )
            tx.change(
                "ingest_request",
                {"request_id": request_id},
                {"rows_reported": len(items), "status": "parsed"},
            )
            tx.put(
                "pipeline_checkpoint",
                {"run_id": run_id, "stage": f"acquisition:{request_id}"},
                {"completed_at": observed},
            )
            self.hook("before_page_commit")

    def write_outputs(self, tx, run_id, query, request_id, items, observed):
        clean, quarantine = [], []
        for occurrence in sorted(
            tx.rows("acquisition_occurrence", request_id=request_id), key=lambda r: r["row_index"]
        ):
            index = occurrence["row_index"]
            if occurrence["outcome"] == "accepted":
                row, reasons, _ = map_item(
                    items[index],
                    {
                        "source": query["provider"],
                        "marketplace_id": query["marketplace_id"],
                        "category": query["category"],
                        "run_id": run_id,
                        "request_id": request_id,
                        "observed_at": observed,
                        "ingested_at": observed,
                    },
                )
                if row is None:
                    raise IntegrityConflict("Accepted row no longer validates")
                clean.append(row)
            elif occurrence["outcome"] == "quarantined":
                quarantine.append(
                    {
                        "raw_request_id": request_id,
                        "row_index": index,
                        "reason_codes": occurrence["reason_codes"],
                        "record": items[index],
                    }
                )
        outputs = (
            (
                "clean",
                self.clean_bucket,
                "parquet",
                encode(clean),
                len(clean),
                "application/vnd.apache.parquet",
            ),
            (
                "quarantine",
                self.quarantine_bucket,
                "json",
                json.dumps(
                    quarantine,
                    sort_keys=True,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                ).encode(),
                len(quarantine),
                "application/json",
            ),
        )
        for kind, bucket, extension, body, count, content_type in outputs:
            key = f"{query['provider']}/{run_id}/{request_id}.{extension}"
            checksum = self.objects.put(bucket, key, body, content_type)
            tx.put(
                "acquisition_output",
                {"request_id": request_id, "kind": kind},
                {
                    "run_id": run_id,
                    "object_bucket": bucket,
                    "object_key": key,
                    "content_sha256": checksum,
                    "row_count": count,
                },
            )
            self.hook("after_" + kind + "_upload")

    def report(self, run_id: str | None = None) -> dict:
        with self.repository.transaction() as tx:
            unique_accepted = tx.count("dim_listing")
            observations = tx.count("fact_price_snapshot")
            if run_id is None:
                return {
                    "dataset_unique_accepted_listings": unique_accepted,
                    "dataset_accepted_observations": observations,
                    "target_unique_valid_listings": 100000,
                    "target_progress_percent": round(unique_accepted / 1000, 4),
                }
            run = tx.rows("pipeline_run", run_id=run_id)[0]
            rows = tx.rows("acquisition_occurrence", run_id=run_id)
            pages = tx.rows("acquisition_page", run_id=run_id)
            requests = tx.rows("ingest_request", run_id=run_id)
            attempts = tx.rows("acquisition_invocation", run_id=run_id)
            outputs = tx.rows("acquisition_output", run_id=run_id)
        counts = {
            name: sum(r["outcome"] == name for r in rows)
            for name in ("accepted", "quarantined", "excluded_duplicate")
        }
        assert len(rows) == sum(counts.values())
        return {
            "label": self.objects.label,
            "run_id": run_id,
            "source": run["source"],
            "status": run["status"],
            "fetched_records": len(rows),
            "fetched_records_scope": "committed_parsed_occurrences; pending RAW excluded",
            "unique_listings": len({r["listing_key"] for r in rows if r["listing_key"]}),
            **counts,
            "raw_objects": len(requests),
            "clean_objects": sum(o["kind"] == "clean" for o in outputs),
            "quarantine_objects": sum(o["kind"] == "quarantine" for o in outputs),
            "output_locations": outputs,
            "raw_locations": [
                {key: request[key] for key in ("object_bucket", "object_key", "content_sha256")}
                for request in requests
            ],
            "pending_raw_pages": sum(p["status"] != "completed" for p in pages),
            "http_attempts": sum(r["http_attempts"] for r in attempts),
            "data_attempts": sum(r["data_attempts"] for r in attempts),
            "dataset_unique_accepted_listings": unique_accepted,
            "dataset_accepted_observations": observations,
            "target_records": 100000,
            "target_unique_listing_percent": round(100 * unique_accepted / 100000, 4),
        }
