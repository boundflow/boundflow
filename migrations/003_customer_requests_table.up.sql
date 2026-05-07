CREATE TYPE customer_request_status AS ENUM (
    'unscheduled',
    'scheduled',
    'in_progress',
    'failed',
    'completed',
    'superceded'
);

CREATE TABLE customer_requests (
    id                    TEXT NOT NULL PRIMARY KEY,
    resource_instance_id  TEXT NOT NULL REFERENCES resource_instances(id),
    status                customer_request_status NOT NULL,
    request_type          TEXT NOT NULL,
    request_info          JSONB NOT NULL DEFAULT '{}',
    current_config_snapshot JSONB NOT NULL DEFAULT '{}',
    goal_config_snapshot    JSONB NOT NULL DEFAULT '{}',
    version                  BIGINT NOT NULL,
    operation_timeout_seconds INTEGER NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);
