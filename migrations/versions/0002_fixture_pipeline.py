"""Durable replay plan, output manifests, checkpoints, and currency-safe views."""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

DDL = """
ALTER TABLE fact_price_snapshot ADD COLUMN change_kind TEXT NOT NULL DEFAULT 'new'
  CHECK (change_kind IN ('new','changed','unchanged'));
CREATE TABLE run_plan (
  run_id TEXT PRIMARY KEY REFERENCES pipeline_run(run_id),
  plan JSONB NOT NULL,
  plan_hash CHAR(64) NOT NULL
);
CREATE TABLE output_artifact (
  artifact_id CHAR(64) PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
  kind TEXT NOT NULL CHECK (kind IN ('clean','quarantine')),
  object_bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  content_sha256 CHAR(64) NOT NULL,
  byte_count BIGINT NOT NULL CHECK (byte_count >= 0),
  row_count BIGINT NOT NULL CHECK (row_count >= 0),
  schema_version INT NOT NULL,
  UNIQUE(run_id, kind),
  UNIQUE(object_bucket, object_key)
);
CREATE TABLE pipeline_checkpoint (
  run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
  stage TEXT NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(run_id, stage)
);
CREATE VIEW mart_listing_counts AS
SELECT r.run_id, l.provider, l.marketplace_id, l.category_canonical,
       count(*) AS listing_count, min(s.observed_at) AS observed_from,
       max(s.observed_at) AS observed_to
FROM fact_price_snapshot s JOIN dim_listing l USING(listing_key)
JOIN pipeline_run r USING(run_id)
GROUP BY r.run_id, l.provider, l.marketplace_id, l.category_canonical;
CREATE VIEW mart_price_ranges AS
SELECT s.run_id, l.provider, l.category_canonical, s.currency_code,
       min(s.price_amount) AS minimum_price, max(s.price_amount) AS maximum_price,
       min(s.observed_at) AS observed_from, max(s.observed_at) AS observed_to
FROM fact_price_snapshot s JOIN dim_listing l USING(listing_key)
GROUP BY s.run_id, l.provider, l.category_canonical, s.currency_code;
CREATE VIEW mart_price_history AS
SELECT s.*, l.provider, l.marketplace_id, l.source_listing_id,
       s.price_amount - lag(s.price_amount) OVER (
         PARTITION BY s.listing_key, s.currency_code ORDER BY s.observed_at, s.run_id
       ) AS price_delta
FROM fact_price_snapshot s JOIN dim_listing l USING(listing_key);
CREATE VIEW mart_quality AS
SELECT run_id, source, stage, metric_name, numerator, denominator, metric_value
FROM quality_metric;
"""


def upgrade() -> None:
    for statement in DDL.split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Destructive downgrade disabled")
