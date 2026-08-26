CREATE TABLE IF NOT EXISTS job_control_watermarks (
    job_name TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    watermark_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by_run_id TEXT,
    PRIMARY KEY (job_name, entity_name)
);

CREATE TABLE IF NOT EXISTS job_control_runs (
    run_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    rows_in BIGINT,
    rows_out BIGINT,
    error_message TEXT
);
