# Baseline audit — 2026-11-03

Read `.ai/Plan.md`, `.ai/Design.md`, `.ai/Task.md` completely before editing.
Existing local edits: acquisition-plan.yaml, requirements.txt, untracked app.py
and config/.backup.yaml. These are user work, not refactor changes.

Baseline command: `.venv/Scripts/python.exe -m pytest -q`:
**33 passed, 1 failed, 2 skipped in 7.71s**. Failure:
`test_plan_has_native_queries_and_provider_bounds` assumes exactly 8 queries;
the user's current plan has 10 eBay queries. Tests must validate bounds and
native queries without fixing a mutable configuration's query count.
Global Python has no pytest; the existing virtualenv is usable.

## Actual implementation

Runtime uses real eBay/Rakuten adapters; tests inject MockTransport and in-memory
transactional repository/S3. Acquisition persists immutable gzip RAW, manifests,
per-page checkpoints, occurrence outcomes, Parquet CLEAN and JSON QUARANTINE.
MART stores listing identity and one price observation per run/listing.
Business hashes normalize decimal scale. Currency is preserved; this is listing
data, not sales or cross-source product matching.

Provider coupling: domain.py Literal source; cleaning.py field names, currency
policy; acquisition.py raw identity extraction; acquisition_plan.py pagination
bounds; runtime.py adapter/credentials/marketplace/diagnostics and fixed workers;
cli.py source choices; migration 0001 source CHECK. Provider-specific branching
in production.py and rakuten_diagnostics.py belongs in adapters. Test factories
also have intentional provider-specific fixture branches. app.py is an existing
user serving UI and outside the shared ingestion core.

eBay retains OAuth, validated next URL/offset, 200 page size/10,000 result cap.
Rakuten retains accessKey/applicationId, website headers, formatVersion 2,
items/Items ambiguity detection and 30 hits/100 pages. Limits require official
documentation verification, not extrapolation between providers.

Migrations 0001–0006 are additive with disabled destructive downgrades. Namespace
owner comments, bucket tags, advisory locks, conditional object writes and SHA256
prevent adoption/overwrite. Recovery reuses RAW before manifest commit; output
uploads precede atomic MART/checkpoint commit. Existing completed legacy pages
can backfill missing output objects. Retention expiry refuses replay.

Compatibility risks: changing hashes/keys breaks existing snapshots; changing
output bytes breaks recovery of orphan uploads; new canonical/mapping versions
must be pinned per run, with an explicit legacy path for old plans. New source
IDs require an additive CHECK replacement, never table recreation. Existing
MART is listing-specific; additional entity kinds need a separate serving design.
Storage tests are opt-in and use retained isolated namespaces. No API acquisition
is authorized by this work plan.
