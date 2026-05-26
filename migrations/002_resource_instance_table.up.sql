CREATE TYPE lifecycle_state AS ENUM (
    'creating',
    'active',
    'reconciling',
    'deleting',
    'deleted',
    'failed'
);

CREATE TYPE workflow_state AS ENUM (
    'created',
    'active',
    'paused',
    'cooldown',
    'disabled',
    'deleted'
);

CREATE TABLE resource_instances (
    id                        TEXT PRIMARY KEY,
    tenant_id                 TEXT NOT NULL REFERENCES tenants(id),
    resource_type             TEXT NOT NULL,
    initial_workflow_version  INTEGER NOT NULL DEFAULT 0,
    current_workflow_version  INTEGER NOT NULL DEFAULT 0,
    invoke_timeout_seconds    INTEGER NOT NULL DEFAULT 0,
    repeat_every_seconds      INTEGER NOT NULL DEFAULT 0,
    triggerable               BOOLEAN NOT NULL DEFAULT true,
    lifecycle_state           lifecycle_state NOT NULL,
    workflow_state            workflow_state NOT NULL DEFAULT 'created',
    lifecycle_policy          JSONB NOT NULL DEFAULT '[]',
    invocation_metrics        JSONB NOT NULL DEFAULT '[]',
    cooldown_until            TIMESTAMPTZ,
    scheduler_partition_id    TEXT,
    target_version            BIGINT NOT NULL DEFAULT 0,
    current_version           BIGINT NOT NULL DEFAULT 0,
    last_completed_request_at TIMESTAMPTZ,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);
