-- DQ metrics emitted by ETL jobs per check, queryable for dashboards and trend analysis.
CREATE TABLE IF NOT EXISTS job_control_dq_metrics (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT,
    job_name    TEXT        NOT NULL,
    check_name  TEXT        NOT NULL,
    metric_name TEXT        NOT NULL,
    metric_value DECIMAL,
    passed      BOOLEAN     NOT NULL DEFAULT TRUE,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_metrics_job ON job_control_dq_metrics (job_name, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_failures ON job_control_dq_metrics (passed, recorded_at DESC) WHERE NOT passed;
