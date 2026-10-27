"""Per-page CLEAN/QUARANTINE manifests; preserves all existing tables/data."""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE acquisition_output (
          request_id TEXT NOT NULL REFERENCES acquisition_page(request_id),
          kind TEXT NOT NULL CHECK (kind IN ('clean','quarantine')),
          run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
          object_bucket TEXT NOT NULL,
          object_key TEXT NOT NULL,
          content_sha256 CHAR(64) NOT NULL,
          row_count BIGINT NOT NULL CHECK (row_count >= 0),
          PRIMARY KEY(request_id, kind),
          UNIQUE(object_bucket, object_key)
        )
    """)
    op.execute("CREATE INDEX acquisition_output_run ON acquisition_output(run_id)")


def downgrade():
    raise RuntimeError("Destructive downgrade disabled")
