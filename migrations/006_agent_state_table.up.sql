CREATE TABLE agent_state (
    resource_instance_id TEXT NOT NULL REFERENCES resource_instances(id) ON DELETE CASCADE,
    agent_name           TEXT NOT NULL,
    runtime_policy       JSONB NOT NULL DEFAULT '{}',
    lifecycle_policy     JSONB NOT NULL DEFAULT '{}',
    -- Rolling circular buffer of invocation metric snapshots.
    -- Each entry: {tokens_used, cost_usd, llm_calls, calls_per_tool, ran_at (epoch ms)}
    invocation_metrics   JSONB NOT NULL DEFAULT '[]',
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_instance_id, agent_name)
);
