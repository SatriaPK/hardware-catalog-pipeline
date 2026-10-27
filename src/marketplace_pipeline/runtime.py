"""Docker runtime wiring. No dotenv reader or administrative approval registry."""

import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event, Lock

from marketplace_pipeline.acquisition import Acquisition
from marketplace_pipeline.acquisition_plan import load_plan
from marketplace_pipeline.adapters.http import ContractError, HttpSession, Policy
from marketplace_pipeline.adapters.production import Credentials, EbayAdapter, RakutenAdapter
from marketplace_pipeline.settings import Settings
from marketplace_pipeline.storage.acquisition import initialize, raw_bucket, verify, worker_lock
from marketplace_pipeline.storage.connections import minio_client, postgres_engine
from marketplace_pipeline.storage.object_store import ObjectStore, safe_segment
from marketplace_pipeline.storage.repository import PostgresRepository


def make_adapter(source, args):
    if not math.isfinite(args.interval) or args.interval < 1:
        raise ValueError("interval must be at least one second")
    credentials = Credentials(
        os.environ.get("EBAY_CLIENT_ID" if source == "ebay" else "RAKUTEN_APPLICATION_ID", ""),
        os.environ.get("EBAY_CLIENT_SECRET" if source == "ebay" else "RAKUTEN_ACCESS_KEY", ""),
    )
    if not credentials.client_id or not credentials.secret:
        raise ValueError("Missing provider credentials")
    session = HttpSession(
        None,
        Policy(
            request_budget=args.max_requests,
            max_retries=0 if args.command == "pilot" else 3,
            interval=args.interval,
            max_wait=max(60, args.interval),
        ),
    )
    return (
        EbayAdapter(session, credentials)
        if source == "ebay"
        else RakutenAdapter(
            session,
            credentials,
            origin=os.environ.get("RAKUTEN_ORIGIN", ""),
            referer=os.environ.get("RAKUTEN_REFERER", ""),
        )
    )


def execute(args):
    source = getattr(args, "source", None)
    market = os.environ.get("EBAY_MARKETPLACE_ID", "EBAY_US") if source == "ebay" else "RAKUTEN_JP"
    adapter = make_adapter(source, args) if source else None
    session = adapter.session if adapter else None
    engine = None
    try:
        settings = Settings.from_environ(os.environ)
        engine = postgres_engine(settings)
        client = minio_client(settings)
        if args.command == "init":
            initialize(engine, client, args.namespace)
            return {"status": "initialized", "namespace": args.namespace}
        verify(engine, client, args.namespace)
        pipeline = Acquisition(
            PostgresRepository(engine, args.namespace),
            ObjectStore(client, label="REAL_API"),
            raw_bucket(args.namespace),
            adapter,
        )
        if args.command == "status":
            return pipeline.report(args.run_id)
        if args.command == "collect":
            adapters = {}
            try:
                for provider in ("ebay", "rakuten"):
                    adapters[provider] = make_adapter(provider, args)
                pipelines = {
                    provider: Acquisition(
                        pipeline.repository, pipeline.objects, pipeline.bucket, api
                    )
                    for provider, api in adapters.items()
                }
                plans = {
                    provider: {
                        **load_plan(
                            args.search_plan,
                            provider,
                            os.environ.get("EBAY_MARKETPLACE_ID", "EBAY_US")
                            if provider == "ebay"
                            else "RAKUTEN_JP",
                        ),
                        "retention_days": args.retention_days,
                    }
                    for provider in adapters
                }
                with worker_lock(engine, args.namespace):
                    return collect(pipelines, args.run_id, plans, args.target, args.max_requests)
            finally:
                for api in adapters.values():
                    api.session.close()
        with worker_lock(engine, args.namespace):
            if args.command != "resume":
                plan = load_plan(args.search_plan, source, market)
                pipeline.prepare(args.run_id, {**plan, "retention_days": args.retention_days})
            elif pipeline.state(args.run_id)[1]["marketplace_id"] != market:
                raise ValueError("Resume marketplace differs from stored plan")
            result = pipeline.run(
                args.run_id, args.max_requests, 1 if args.command == "pilot" else args.max_requests
            )
        result["http_status"] = session.events[-1]["status"] if session.events else None
        if isinstance(adapter, RakutenAdapter) and result["status"] == "failed":
            result.update(
                provider_error=adapter.provider_error,
                response_structure=adapter.response_structure,
                schema_violations=adapter.schema_violations,
            )
        return result
    finally:
        if session is not None:
            session.close()
        if engine is not None:
            engine.dispose()


def collect(pipelines, run_id, plans, target, max_requests, *, emit=None):
    """Two provider workers, one namespace and one shared HTTP-attempt budget."""
    if emit is None:

        def emit(event):
            print(json.dumps(event, ensure_ascii=True), flush=True)

    budget_lock, output_lock = Lock(), Lock()
    stopped = Event()
    attempts = [0]
    started = time.monotonic()
    first = next(iter(pipelines.values()))

    def count():
        with first.repository.transaction() as tx:
            return tx.count("dim_listing")

    def stop():
        if stopped.is_set():
            return "collection_stopped"
        return "target_reached" if count() >= target else None

    def reserve():
        with budget_lock:
            if attempts[0] >= max_requests:
                raise ContractError("request_budget_exhausted")
            attempts[0] += 1

    identifiers = {source: safe_segment(f"{run_id}-{source}") for source in pipelines}
    for source, pipeline in pipelines.items():
        with pipeline.repository.transaction() as tx:
            exists = bool(tx.rows("pipeline_run", run_id=identifiers[source]))
        if exists:
            _, stored = pipeline.state(identifiers[source])
            if stored["marketplace_id"] != plans[source]["marketplace_id"]:
                raise ValueError("Stored marketplace differs from configuration")
        else:
            pipeline.prepare(identifiers[source], plans[source])

    def progress(source, counts):
        with output_lock:
            emit(
                {
                    "event": "progress",
                    "source": source,
                    "unique_valid_listings": count(),
                    "target": target,
                    "http_attempts_this_invocation": attempts[0],
                    "page_counts": counts,
                    "elapsed_seconds": round(time.monotonic() - started, 1),
                }
            )

    def worker(source, pipeline):
        try:
            result = pipeline.run(
                identifiers[source],
                max_requests,
                max_requests,
                stop=stop,
                before_request=reserve,
                progress=lambda counts: progress(source, counts),
            )
            if result["status"] == "failed":
                stopped.set()
            summary = {
                key: result[key]
                for key in (
                    "run_id",
                    "status",
                    "stop_reason",
                    "fetched_records",
                    "accepted",
                    "quarantined",
                    "excluded_duplicate",
                    "http_attempts",
                )
            }
            summary["http_status"] = (
                pipeline.adapter.session.events[-1]["status"]
                if pipeline.adapter.session.events
                else None
            )
            if isinstance(pipeline.adapter, RakutenAdapter) and result["status"] == "failed":
                summary.update(
                    provider_error=pipeline.adapter.provider_error,
                    response_structure=pipeline.adapter.response_structure,
                    schema_violations=pipeline.adapter.schema_violations,
                )
            return summary
        except BaseException:
            stopped.set()
            raise

    results = {}
    emit(
        {
            "event": "started",
            "run_ids": identifiers,
            "unique_valid_listings": count(),
            "target": target,
            "max_requests_combined": max_requests,
        }
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            pool.submit(worker, source, pipeline): source for source, pipeline in pipelines.items()
        }
        try:
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        except BaseException:
            stopped.set()
            raise
    total = count()
    failed = any(result["status"] == "failed" for result in results.values())
    reason = (
        "worker_failed"
        if failed
        else "target_reached"
        if total >= target
        else "queries_exhausted"
        if all(r["status"] == "completed" for r in results.values())
        else "request_budget_exhausted"
    )
    return {
        "status": "failed" if failed else "completed" if total >= target else "partial",
        "stop_reason": reason,
        "unique_valid_listings": total,
        "target": target,
        "http_attempts_this_invocation": attempts[0],
        "sources": results,
    }
