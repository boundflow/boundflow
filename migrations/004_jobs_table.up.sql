CREATE TYPE job_status AS ENUM (
    'pending',
    'dispatched',
    'running',
    'awaiting_next',
    'awaiting_approval',
    'approved',
    'rejected',
    'completed',
    'failed'
);

CREATE TABLE jobs (
    workflow_id     TEXT PRIMARY KEY REFERENCES workflows(id),
    request_id               TEXT NOT NULL,
    version                  BIGINT NOT NULL,
    current_atomic_operation TEXT NOT NULL,
    context                  JSONB NOT NULL DEFAULT '{}',
    status                   job_status NOT NULL,
    job_type                 TEXT NOT NULL,
    workflow_type            TEXT NOT NULL,
    timeout_seconds          INTEGER NOT NULL,
    workflow_version         INTEGER NOT NULL DEFAULT 0,
    agent_metrics            JSONB NOT NULL DEFAULT '{}',
    workflow_metrics         JSONB NOT NULL DEFAULT '{}',
    -- Customer-facing result of the run, NULL until the job reaches 'completed'.
    result_type              run_outcome,
    failure_reason           TEXT NOT NULL DEFAULT '',
    -- The run's published output (Complete(result=...)), NULL until completed and
    -- NULL if the workflow never published one.
    result                    JSONB,
    owner                    TEXT,
    lease_expires_at         TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Populated when the job carries extra server-internal state (e.g. approval gate branches).
    job_metadata             JSONB NOT NULL DEFAULT '{}',
    -- Approval gate: only populated when status = awaiting_approval/approved/rejected.
    approval_id              TEXT,
    approval_opened_at       TIMESTAMPTZ,
    approval_timeout_at      TIMESTAMPTZ,
    -- Published for external readers while the gate is open (Workflow.pending_approval).
    approval_justification   TEXT NOT NULL DEFAULT '',
    approval_metadata        JSONB
);
