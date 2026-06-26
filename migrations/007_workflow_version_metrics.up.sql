CREATE TABLE workflow_version_metrics (
    workflow_id  TEXT NOT NULL REFERENCES workflows(id),
    version               INTEGER NOT NULL,
    -- epoch increments each time the workflow transitions TO this version,
    -- so version 1 → 2 → 1 produces two distinct rows for version 1.
    epoch                 INTEGER NOT NULL DEFAULT 1,
    total_cost            NUMERIC(12, 6) NOT NULL DEFAULT 0,
    run_count             INTEGER NOT NULL DEFAULT 0,
    total_failures        INTEGER NOT NULL DEFAULT 0,
    total_llm_calls       INTEGER NOT NULL DEFAULT 0,
    total_latency_seconds NUMERIC(12, 3) NOT NULL DEFAULT 0,
    total_approval_rejections INTEGER NOT NULL DEFAULT 0,
    tool_failure_counts   JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (workflow_id, version, epoch)
);
