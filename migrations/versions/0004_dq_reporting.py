"""Add versioned DQ evaluations; retain all legacy metrics and storage contracts."""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None

DDL = """
CREATE TABLE dq_evaluation (
  run_id TEXT NOT NULL REFERENCES pipeline_run(run_id),
  stage TEXT NOT NULL CHECK (stage IN ('raw_parsed','clean','mart')),
  rule_version TEXT NOT NULL,
  evaluated_at_utc TIMESTAMPTZ NOT NULL,
  input_fingerprint CHAR(64) NOT NULL,
  PRIMARY KEY (run_id, stage, rule_version)
);
CREATE TABLE dq_metric (
  run_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  rule_version TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  numerator BIGINT NOT NULL CHECK (numerator >= 0),
  denominator BIGINT CHECK (denominator >= 0),
  CHECK (denominator IS NULL OR numerator <= denominator),
  PRIMARY KEY (run_id, stage, rule_version, metric_name),
  FOREIGN KEY (run_id, stage, rule_version)
    REFERENCES dq_evaluation(run_id, stage, rule_version)
);
CREATE VIEW mart_dq_report AS
SELECT m.run_id, r.source, r.marketplace_id, m.stage,
       r.started_at AS observed_at_utc, e.evaluated_at_utc, m.rule_version,
       m.metric_name, m.numerator, m.denominator,
       CASE WHEN m.denominator IS NULL THEN m.numerator::numeric
            WHEN m.denominator = 0 THEN NULL
            ELSE round(100.0 * m.numerator / m.denominator, 8) END AS metric_value
FROM dq_metric m JOIN dq_evaluation e USING (run_id, stage, rule_version)
JOIN pipeline_run r USING (run_id);
"""


def upgrade() -> None:
    for statement in DDL.split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Destructive downgrade disabled; use a reviewed forward migration")
