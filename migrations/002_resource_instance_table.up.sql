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
    initial_version          INTEGER NOT NULL DEFAULT 0,
    invoke_timeout_seconds   INTEGER NOT NULL DEFAULT 0,
    repeat_every_seconds     INTEGER NOT NULL DEFAULT 0,
    triggerable              BOOLEAN NOT NULL DEFAULT true,
    lifecycle_state          lifecycle_state NOT NULL,
    scheduler_partition_id   TEXT,
    target_version           BIGINT NOT NULL DEFAULT 0,
    current_version          BIGINT NOT NULL DEFAULT 0,
    last_completed_request_at TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);
