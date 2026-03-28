CREATE TYPE customer_request_status AS ENUM (
    'unscheduled',
    'scheduled',
    'in_progress',
    'failed',
    'completed',
    'superceded'
);

CREATE TABLE customer_requests (
    id                    TEXT NOT NULL,
    resource_instance_id  TEXT NOT NULL REFERENCES resource_instances(id),
    superceded_request_id TEXT,
    status                customer_request_status NOT NULL DEFAULT 'pending',
    request_type          TEXT NOT NULL,
    request_info          JSONB NOT NULL DEFAULT '{}',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (resource_instance_id, id)
);
