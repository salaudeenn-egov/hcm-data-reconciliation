-- Run once per environment to create the audit table.
-- For central instance (schema-prefixed): SET search_path = so; before running.
-- For individual instance (Mozambique): run as-is in public schema.

CREATE TABLE IF NOT EXISTS recon_run_log (
    id               BIGSERIAL    PRIMARY KEY,
    run_id           VARCHAR(16)  NOT NULL,
    job_name         VARCHAR(255) NOT NULL,
    tenant_id        VARCHAR(50),
    es_index         VARCHAR(255),
    kafka_topic      VARCHAR(255),
    started_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    status           VARCHAR(20)  NOT NULL DEFAULT 'RUNNING',  -- RUNNING | COMPLETED | FAILED
    db_count         INTEGER,
    es_count         INTEGER,
    matched_count    INTEGER,
    missing_count    INTEGER,
    fetched_count    INTEGER,
    dropped_count    INTEGER,
    not_in_api_count INTEGER,
    pushed_count     INTEGER,     -- NULL in dry run
    api_failed_ids   INTEGER,
    kafka_failures   INTEGER,     -- NULL in dry run
    error_message    TEXT
);

CREATE INDEX IF NOT EXISTS idx_recon_run_log_run_id   ON recon_run_log (run_id);
CREATE INDEX IF NOT EXISTS idx_recon_run_log_job_name ON recon_run_log (job_name);
CREATE INDEX IF NOT EXISTS idx_recon_run_log_started  ON recon_run_log (started_at DESC);

-- Useful queries for operators:

-- Latest run per job:
-- SELECT DISTINCT ON (job_name) * FROM recon_run_log ORDER BY job_name, started_at DESC;

-- All failed runs:
-- SELECT * FROM recon_run_log WHERE status = 'FAILED' ORDER BY started_at DESC;

-- Runs still marked RUNNING (crashed mid-run):
-- SELECT * FROM recon_run_log WHERE status = 'RUNNING' AND started_at < NOW() - INTERVAL '2 hours';
