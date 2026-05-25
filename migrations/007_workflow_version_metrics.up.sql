CREATE TABLE workflow_version_metrics (
    resource_instance_id  TEXT NOT NULL REFERENCES resource_instances(id),
    version               INTEGER NOT NULL,
    total_cost            NUMERIC(12, 6) NOT NULL DEFAULT 0,
    run_count             INTEGER NOT NULL DEFAULT 0,
    total_failures        INTEGER NOT NULL DEFAULT 0,
    total_llm_calls       INTEGER NOT NULL DEFAULT 0,
    total_latency_seconds NUMERIC(12, 3) NOT NULL DEFAULT 0,
    total_approval_rejections INTEGER NOT NULL DEFAULT 0,
    tool_failure_counts   JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (resource_instance_id, version)
);
