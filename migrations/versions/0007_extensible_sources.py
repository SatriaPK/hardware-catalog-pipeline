"""Allow registered source identifiers without rewriting existing data."""

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade():
    # Exact constraint installed by 0001. No tables, rows or indexes are dropped.
    op.execute("ALTER TABLE pipeline_run DROP CONSTRAINT pipeline_run_source_check")
    op.execute("""
        ALTER TABLE pipeline_run ADD CONSTRAINT pipeline_run_source_check
        CHECK (source ~ '^[a-z][a-z0-9_-]{0,63}$') NOT VALID
    """)
    op.execute("ALTER TABLE pipeline_run VALIDATE CONSTRAINT pipeline_run_source_check")


def downgrade():
    raise RuntimeError("Source narrowing could invalidate data; use reviewed forward migration")
