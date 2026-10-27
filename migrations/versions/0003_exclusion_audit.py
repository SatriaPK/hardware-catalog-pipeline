"""Preserve provenance of occurrences excluded by deterministic deduplication."""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE excluded_occurrence (
          exclusion_id CHAR(64) PRIMARY KEY,
          run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
          raw_request_id TEXT NOT NULL,
          row_index INT NOT NULL CHECK (row_index >= 0),
          accepted_request_id TEXT NOT NULL,
          accepted_row_index INT NOT NULL CHECK (accepted_row_index >= 0),
          listing_key CHAR(64) NOT NULL,
          reason_code TEXT NOT NULL CHECK (reason_code = 'duplicate_same_listing'),
          UNIQUE (run_id, raw_request_id, row_index),
          FOREIGN KEY (run_id, raw_request_id) REFERENCES ingest_request(run_id, request_id),
          FOREIGN KEY (run_id, accepted_request_id) REFERENCES ingest_request(run_id, request_id)
        )
    """)


def downgrade() -> None:
    raise RuntimeError("Destructive downgrade disabled")
