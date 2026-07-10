CREATE TYPE lifecycle_state AS ENUM (
    'creating',
    'active',
    'scheduled',
    'blocked',
    'invoking',
    'awaiting_approval',
    'awaiting_input',
    'deleting',
    'deleted',
    'interrupted'
);

CREATE TYPE workflow_state AS ENUM (
    'active',
    'paused',
    'cooldown',
    'disabled'
);

CREATE TABLE workflows (
    id                        TEXT PRIMARY KEY,
    tenant_id                 TEXT NOT NULL REFERENCES tenants(id),
    workflow_type             TEXT NOT NULL,
    current_workflow_version  INTEGER NOT NULL DEFAULT 0,
    invoke_timeout_seconds    INTEGER NOT NULL DEFAULT 0,
    repeat_every_seconds      INTEGER NOT NULL DEFAULT 0,
    triggerable               BOOLEAN NOT NULL DEFAULT true,
    invoke_mode               TEXT NOT NULL DEFAULT 'coalesce',
    max_queue_depth           INTEGER NOT NULL DEFAULT 0,
    lifecycle_state           lifecycle_state NOT NULL,
    workflow_state            workflow_state NOT NULL DEFAULT 'paused',
    lifecycle_policy          JSONB NOT NULL DEFAULT '[]',
    invocation_metrics        JSONB NOT NULL DEFAULT '[]',
    cooldown_until            TIMESTAMPTZ,
    lifecycle_last_resolved   BIGINT NOT NULL DEFAULT 0,
    scheduler_partition_id    TEXT,
    target_version            BIGINT NOT NULL DEFAULT 0,
    current_version           BIGINT NOT NULL DEFAULT 0,
    metrics_emitted_at        BIGINT NOT NULL DEFAULT 0,
    last_completed_request_at TIMESTAMPTZ,
    last_interrupted_request_id    TEXT NOT NULL DEFAULT '',
    last_gate_id           TEXT,
    last_gate_detail       TEXT,
    last_gate_metadata     JSONB,
    last_gate_opened_at    TIMESTAMPTZ,
    last_gate_timeout_at   TIMESTAMPTZ,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);
