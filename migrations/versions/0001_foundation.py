"""Frozen PostgreSQL 16 schema; never auto-applied."""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

DDL = """
CREATE TABLE pipeline_run (
  run_id TEXT PRIMARY KEY,
  source TEXT NOT NULL CHECK (source IN ('ebay','rakuten','fixture')),
  marketplace_id TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('fixture','sandbox','live','raw')),
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  status TEXT NOT NULL CHECK (status IN ('pending','running','partial','failed','completed')),
  query_plan_hash TEXT,
  error_summary TEXT
);

CREATE TABLE ingest_request (
  request_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
  source TEXT NOT NULL,
  marketplace_id TEXT NOT NULL,
  query_id TEXT NOT NULL,
  page_number INT,
  endpoint_version TEXT,
  safe_query_hash CHAR(64),
  byte_count BIGINT,
  schema_version INT NOT NULL DEFAULT 1,
  observed_at TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL,
  object_bucket TEXT,
  object_key TEXT,
  content_sha256 CHAR(64),
  http_status INT,
  rows_reported BIGINT,
  status TEXT NOT NULL,
  UNIQUE (run_id, request_id)
);

CREATE TABLE dim_marketplace (
  marketplace_id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  region TEXT NOT NULL
);

CREATE TABLE dim_product (
  product_key CHAR(64) PRIMARY KEY,
  normalized_name TEXT,
  brand TEXT,
  model TEXT,
  variant TEXT,
  gtin TEXT,
  match_method TEXT,
  review_status TEXT NOT NULL
);

CREATE TABLE dim_listing (
  listing_key CHAR(64) PRIMARY KEY,
  provider TEXT NOT NULL,
  marketplace_id TEXT NOT NULL REFERENCES dim_marketplace(marketplace_id),
  source_listing_id TEXT NOT NULL,
  product_key CHAR(64) REFERENCES dim_product(product_key),
  title_original TEXT NOT NULL,
  category_canonical TEXT,
  first_seen_at TIMESTAMPTZ,
  last_seen_at TIMESTAMPTZ,
  UNIQUE (provider, marketplace_id, source_listing_id)
);

CREATE TABLE fact_price_snapshot (
  snapshot_key CHAR(64) PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
  listing_key CHAR(64) NOT NULL REFERENCES dim_listing(listing_key),
  raw_request_id TEXT NOT NULL REFERENCES ingest_request(request_id),
  observed_at TIMESTAMPTZ NOT NULL,
  price_amount NUMERIC(18,4) NOT NULL CHECK (price_amount >= 0),
  currency_code CHAR(3) NOT NULL,
  record_hash CHAR(64) NOT NULL,
  UNIQUE (run_id, listing_key),
  FOREIGN KEY (run_id, raw_request_id) REFERENCES ingest_request(run_id, request_id)
);

CREATE TABLE quality_metric (
  metric_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
  source TEXT NOT NULL,
  stage TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  numerator BIGINT,
  denominator BIGINT,
  metric_value DOUBLE PRECISION,
  measured_at TIMESTAMPTZ NOT NULL,
  UNIQUE (run_id, source, stage, metric_name)
);

CREATE TABLE quarantine_issue (
  issue_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
  raw_request_id TEXT REFERENCES ingest_request(request_id),
  source_listing_id TEXT,
  reason_code TEXT NOT NULL,
  field_name TEXT,
  safe_detail TEXT,
  object_bucket TEXT,
  object_key TEXT,
  occurred_at TIMESTAMPTZ NOT NULL
);
"""


def upgrade() -> None:
    for statement in DDL.split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Destructive downgrade disabled; review a forward migration")
