CREATE TYPE job_status AS ENUM (
    'pending',
    'running',
    'completed',
    'failed'
);

CREATE TABLE jobs (
    resource_instance_id    TEXT PRIMARY KEY REFERENCES resource_instances(id),
    current_atomic_operation TEXT NOT NULL,
    context                 JSONB NOT NULL DEFAULT '{}',
    status                  job_status NOT NULL DEFAULT 'pending',
    job_type                TEXT NOT NULL,
    owner                   TEXT,
    lease_expires_at        TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
