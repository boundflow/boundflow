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
    lifecycle_state          lifecycle_state NOT NULL,
    scheduler_partition_id   TEXT,
    target_version           BIGINT NOT NULL DEFAULT 0,
    current_version          BIGINT NOT NULL DEFAULT 0,
    last_completed_request_at TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);
