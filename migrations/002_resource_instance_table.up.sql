CREATE TYPE lifecycle_state AS ENUM (
    'creating',
    'active',
    'reconciling',
    'deleting',
    'deleted',
    'failed'
);

CREATE TABLE resource_instances (
    id                       TEXT PRIMARY KEY,
    tenant_id                TEXT NOT NULL REFERENCES tenants(id),
    resource_type            TEXT NOT NULL,
    current_config_state     JSONB NOT NULL DEFAULT '{}',
    config_goal_state        JSONB NOT NULL DEFAULT '{}',
    lifecycle_state          lifecycle_state NOT NULL DEFAULT 'creating',
    scheduler_partition_id   TEXT,
    locked                   BOOLEAN NOT NULL DEFAULT false,
    last_completed_request_at TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);
