"""Bounded real acquisition state; additive, no modifications to fixture contracts."""

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None

DDL = """
CREATE TABLE acquisition_invocation (
  invocation_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
  started_at TIMESTAMPTZ NOT NULL,
  max_requests INT NOT NULL CHECK (max_requests > 0),
  max_data_requests INT NOT NULL CHECK (max_data_requests > 0),
  http_attempts INT NOT NULL DEFAULT 0,
  data_attempts INT NOT NULL DEFAULT 0
);
CREATE TABLE acquisition_page (
  request_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
  query_id TEXT NOT NULL,
  page_number INT NOT NULL CHECK (page_number > 0),
  status TEXT NOT NULL CHECK (status IN ('prepared','durable','completed')),
  next_page INT,
  rows_fingerprint CHAR(64),
  UNIQUE(run_id,query_id,page_number)
);
CREATE TABLE acquisition_occurrence (
  request_id TEXT NOT NULL REFERENCES acquisition_page(request_id),
  row_index INT NOT NULL,
  run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
  listing_key CHAR(64),
  outcome TEXT NOT NULL CHECK (outcome IN ('accepted','quarantined','excluded_duplicate')),
  reason_codes JSONB NOT NULL,
  PRIMARY KEY (request_id,row_index)
);
CREATE INDEX acquisition_occurrence_run ON acquisition_occurrence(run_id);
CREATE INDEX snapshot_listing_observation ON fact_price_snapshot(listing_key, observed_at);
CREATE VIEW acquisition_progress AS
SELECT r.run_id,r.source,r.marketplace_id,r.status,
 (SELECT count(*) FROM acquisition_occurrence o WHERE o.run_id=r.run_id) AS fetched_records,
 (SELECT count(DISTINCT listing_key) FROM acquisition_occurrence o
    WHERE o.run_id=r.run_id) AS unique_listings,
 (SELECT count(*) FROM acquisition_occurrence o
    WHERE o.run_id=r.run_id AND outcome='accepted') AS accepted,
 (SELECT count(*) FROM acquisition_occurrence o
    WHERE o.run_id=r.run_id AND outcome='quarantined') AS quarantined,
 (SELECT count(*) FROM acquisition_occurrence o
    WHERE o.run_id=r.run_id AND outcome='excluded_duplicate') AS excluded_duplicates,
 (SELECT count(*) FROM ingest_request i WHERE i.run_id=r.run_id) AS raw_objects,
 (SELECT coalesce(sum(http_attempts),0) FROM acquisition_invocation i
    WHERE i.run_id=r.run_id) AS http_attempts
FROM pipeline_run r WHERE r.mode='live';
"""


def upgrade() -> None:
    for statement in DDL.split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Destructive downgrade disabled")
