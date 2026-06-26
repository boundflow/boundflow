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
    workflow_id  TEXT NOT NULL REFERENCES workflows(id),
    status                customer_request_status NOT NULL,
    request_type          TEXT NOT NULL,
    request_info          JSONB NOT NULL DEFAULT '{}',
    version               BIGINT NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at             TIMESTAMPTZ
);
