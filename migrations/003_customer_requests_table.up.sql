CREATE TYPE customer_request_status AS ENUM (
    'unscheduled',
    'scheduled',
    'in_progress',
    'failed',
    'completed',
    'superceded'
);

-- Customer-facing outcome of a request, set once it is terminal (the in-flight state is
-- described by request status). Only 'interrupted' corresponds to a failed request
-- status; the other non-successful outcomes are completed requests
CREATE TYPE run_outcome AS ENUM (
    'successful',
    'customer_marked_failure',
    'uncaught_operation_exception',
    'operation_timeout',
    'interrupted'
);

CREATE TABLE customer_requests (
    id                    TEXT NOT NULL PRIMARY KEY,
    workflow_id  TEXT NOT NULL REFERENCES workflows(id),
    status                customer_request_status NOT NULL,
    request_type          TEXT NOT NULL,
    request_info          JSONB NOT NULL DEFAULT '{}',
    version               BIGINT NOT NULL,
    -- NULL until the request is terminal; request status covers the in-flight state.
    run_outcome           run_outcome,
    failure_reason        TEXT NOT NULL DEFAULT '',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at             TIMESTAMPTZ
);
